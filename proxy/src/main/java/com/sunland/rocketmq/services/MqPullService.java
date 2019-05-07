package com.sunland.rocketmq.services;

import com.google.common.base.Joiner;
import com.sunland.rocketmq.db.Batcher;
import com.sunland.rocketmq.model.InternalKey;
import com.sunland.rocketmq.model.InternalValue;
import com.sunland.rocketmq.utils.JsonUtils;
import org.apache.rocketmq.client.consumer.MQPullConsumerScheduleService;
import org.apache.rocketmq.client.consumer.MQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullTaskContext;
import org.apache.rocketmq.client.consumer.PullTaskCallback;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


public class MqPullService implements Runnable {

    private static final String CONSUMER_GROUP = "SCHEDULE_GROUP";
    private static final String SCHEDLE_TOPIC = "SCHEDULE_XXXX";
    private MQPullConsumerScheduleService scheduleService;
    private String mqPullServiceName;
    private volatile boolean shouldStop = false;
    private static final Batcher BATCHER = Batcher.getInstance();

    private final int internalQueueCount = 5000;
    private final BlockingQueue<InternalValue> blockingQueue = new ArrayBlockingQueue<>(internalQueueCount);


    private static final Logger LOGGER = LoggerFactory.getLogger(MqPullService.class);

    public MqPullService(final int index) {
        this.mqPullServiceName = Joiner.on("-").join("mqPullServiceName", index);
        scheduleService = new MQPullConsumerScheduleService(CONSUMER_GROUP);
        initScheduleService();
        LOGGER.info("{} init  consumer, consumerGroup:{}", mqPullServiceName, CONSUMER_GROUP);

    }

    public void initScheduleService() {
        scheduleService.setMessageModel(MessageModel.CLUSTERING);
        scheduleService.registerPullTaskCallback(SCHEDLE_TOPIC, new PullTaskCallback() {

            @Override
            public void doPullTask(MessageQueue mq, PullTaskContext context) {
                MQPullConsumer consumer = context.getPullConsumer();
                try {

                    long offset = consumer.fetchConsumeOffset(mq, false);
                    if (offset < 0)
                        offset = 0;

                    PullResult pullResult = consumer.pull(mq, "*", offset, 32);
                    MqPullService.this.addMessage(pullResult);

                    consumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset());

                    context.setPullNextDelayTimeMillis(100);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        run();

    }

    private void addMessage(PullResult pullResult) {
        switch (pullResult.getPullStatus()) {
            case FOUND:
                for (MessageExt msgExt : pullResult.getMsgFoundList()) {
                    InternalKey key = new InternalKey(msgExt.getBornTimestamp() + msgExt.getDelayTime());
                    InternalValue value = new InternalValue(msgExt);
                    BATCHER.checkAndPut(key, JsonUtils.toJsonString(value));
                }
                if (blockingQueue.size() != 0 && blockingQueue.size() % internalQueueCount == 0) {
                    MqPushService.getInstance().sendConcurrent(blockingQueue, this.mqPullServiceName);
                }
                break;
            case NO_MATCHED_MSG:
                break;
            case NO_NEW_MSG:
            case OFFSET_ILLEGAL:
                break;
            default:
                break;
        }
    }

    @Override
    public void run() {
        while (!shouldStop) {
            try {
                scheduleService.start();
            } catch (Exception e) {
                LOGGER.error("exception happened", e);
            }
        }
    }

    public void start() {
        new Thread(this).start();
    }

    public void stop() {
        final long start = System.currentTimeMillis();
        LOGGER.info("schedule consumer will stop ...");
        shouldStop = true;
        scheduleService.shutdown();
        LOGGER.info("{} carrera consumer has stopped, cost:{}ms", mqPullServiceName, System.currentTimeMillis() - start);
    }
}
