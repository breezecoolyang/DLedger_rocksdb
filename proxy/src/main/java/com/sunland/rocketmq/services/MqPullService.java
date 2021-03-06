package com.sunland.rocketmq.services;

import com.sunland.rocketmq.db.Batcher;
import com.sunland.rocketmq.model.InternalKey;
import com.sunland.rocketmq.model.InternalValue;
import com.sunland.rocketmq.utils.JsonUtils;
import com.sunland.rocketmq.utils.MsgUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.sunland.rocketmq.config.ConfigManager;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


public class MqPullService implements Runnable {

    private static final String CONSUMER_GROUP = "schedule_consumer_group";
    private static final String SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";
    private DefaultMQPushConsumer consumer;
    private String mqPullServiceName;
    private volatile boolean shouldStop = false;
    private static final Batcher BATCHER = Batcher.getInstance();

    private final int internalQueueCount = 5000;
    private final BlockingQueue<InternalValue> blockingQueue = new ArrayBlockingQueue<>(internalQueueCount);


    private static final Logger LOGGER = LoggerFactory.getLogger(MqPullService.class);

    public MqPullService() {
        this.mqPullServiceName = "mqPullServiceName";
        consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        initConsumer();
    }

    private void initConsumer() {
        try {
            consumer.subscribe(SCHEDULE_TOPIC, "*");
            consumer.setConsumeMessageBatchMaxSize(ConfigManager.getConfig().getConsumeMessageBatchMaxSize());
            consumer.setNamesrvAddr(ConfigManager.getConfig().getNameServeAddr());
            consumer.registerMessageListener(new MessageListenerConcurrently() {

                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {

                    try {
                        MqPullService.LOGGER.info("get message in consumeMessage , msg size is {}", msgs.size());
                        for (MessageExt msgExt : msgs) {

                            InternalKey key = new InternalKey(MqPullService.computeDeliverTimestamp(msgExt));
                            MsgUtils.clearExtralProperties(msgExt);
                            InternalValue value = new InternalValue(msgExt);
                            MqPullService.LOGGER.info("ready to put message in consumeMessage, key is {}, value is {}", key, value);
                            if (BATCHER.checkAndPut(key, JsonUtils.toJsonString(value))) {
                                continue;
                            }
                            // delay time is up, should send the msg directly
                            putToBlockingQueue(value);
                            if (blockingQueue.size() != 0 && blockingQueue.size() % internalQueueCount == 0) {
                                MqPushService.getInstance().sendConcurrent(blockingQueue, MqPullService.this.mqPullServiceName);
                            }
                        }

                        BATCHER.flush();

                        if (blockingQueue.size() != 0) {
                            MqPushService.getInstance().sendConcurrent(blockingQueue, MqPullService.this.mqPullServiceName);
                        }
                    } catch (Exception e) {
                        MqPullService.LOGGER.error("exception in consume message", e);
                    }

                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            LOGGER.info("{} init  consumer, consumerGroup:{}", mqPullServiceName, CONSUMER_GROUP);
        } catch (Exception e) {
            LOGGER.error(" init  consumer exception", e);
        }
    }


    private static long computeDeliverTimestamp(MessageExt msgExt) {
        return msgExt.getStoreTimestamp() / 1000 + msgExt.getDelayTime();
    }

    private void putToBlockingQueue(InternalValue internalValue) {
        try {
            blockingQueue.put(internalValue);
        } catch (InterruptedException e) {
            LOGGER.error("error while put to blockingQueue");
        }
    }

    @Override
    public void run() {
        try {
            consumer.start();
        } catch (Exception e) {
            LOGGER.error("exception happened", e);
        }
    }

    public void start() {
        new Thread(this).start();
    }


    public void stop() {
        final long start = System.currentTimeMillis();
        LOGGER.warn("schedule consumer will stop ...");
        shouldStop = true;
        consumer.shutdown();
        Batcher.close();
        LOGGER.warn("{}  consumer has stopped, cost:{}ms", mqPullServiceName, System.currentTimeMillis() - start);
    }
}
