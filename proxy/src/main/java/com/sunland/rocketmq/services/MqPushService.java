package com.sunland.rocketmq.services;

import com.google.common.base.Charsets;
import com.sunland.rocketmq.config.SeekTimeConfig;
import com.sunland.rocketmq.db.DbOperation;
import com.sunland.rocketmq.entry.DLedgerEntry;
import com.sunland.rocketmq.model.InternalValue;
import com.sunland.rocketmq.protocol.GetListEntriesResponse;
import com.sunland.rocketmq.utils.JsonUtils;
import com.sunland.rocketmq.utils.TsUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;

public class MqPushService {
    private static final Logger LOGGER = LoggerFactory.getLogger(MqPushService.class);
    private static final String PRODUCER_GROUP = "producer_group";
    private static final int BATCH_SEND_THREAD_NUM = 10;
    private static final int PUSH_QUEUE_SIZE = 100000;
    private static final int DEFAULT_CAPACITY = 100000;
    private static final int INTERNAL_PAIR_COUNT = 100000;
    private static final BlockingQueue<InternalValue> BLOCKING_QUEUE = new ArrayBlockingQueue<>(INTERNAL_PAIR_COUNT);
    private static volatile MqPushService instance = null;
    private DefaultMQProducer producer = null;
    private long round = 0;
    private DbOperation dbOperation;

    /**
     * send from db
     */
    private final ThreadPoolExecutor pushThreadPool = new ThreadPoolExecutor(BATCH_SEND_THREAD_NUM, BATCH_SEND_THREAD_NUM,
        30L, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(PUSH_QUEUE_SIZE),
        new BasicThreadFactory.Builder().namingPattern("send-normal-to-pproxy-%d").build(),
        (r, executor) -> {
            try {
                executor.getQueue().put(r);
            } catch (InterruptedException e) {
                LOGGER.info("error while reject execution put to queue, err:{}", e.getMessage(), e);
            }
        });

    /**
     * send directly
     */
    private final ThreadPoolExecutor directPushThreadPool = new ThreadPoolExecutor(BATCH_SEND_THREAD_NUM, BATCH_SEND_THREAD_NUM,
        30L, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(PUSH_QUEUE_SIZE),
        new BasicThreadFactory.Builder().namingPattern("send-direct-to-pproxy-%d").build(),
        (r, executor) -> {
            try {
                executor.getQueue().put(r);
            } catch (InterruptedException e) {
                LOGGER.info("error while reject execution put to queue, err:{}", e.getMessage(), e);
            }
        });

    private MqPushService() {
        initProducer();
        dbOperation = new DbOperation();
    }

    private void initProducer() {
        final long start = System.currentTimeMillis();
        producer = new DefaultMQProducer(PRODUCER_GROUP);

        try {
            producer.start();
            LOGGER.info("succ start producer, cost:{}ms", System.currentTimeMillis() - start);
        } catch (Exception e) {
            LOGGER.error("error while start producer, err:{}", e.getMessage(), e);
        }
    }

    public void pullFromDefaultCFAndPush() {
        final long seekTimestamp = SeekTimeConfig.getSeekTime();

        // seekTimestamp can't exceed current time
        final long now = TsUtils.genTS();
        if (seekTimestamp > now) {
            LOGGER.debug("pull from db should stop for seekTimestamp > now, seekTimestamp:{}, now:{}, Thread:{}",
                    seekTimestamp, now, Thread.currentThread().getName());
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
            }
            return;
        }

        round++;
        final long start = System.currentTimeMillis();
        final long diff = start / 1000 - seekTimestamp;
        LOGGER.info("pull from db start, seekTimestamp:{}, currTimestamp:{}, diff:{} round:{}",
                seekTimestamp, start / 1000, diff, round);

        int count = 0;
        GetListEntriesResponse listEntriesResponse = dbOperation.getByTime(now);
        for (DLedgerEntry entry : listEntriesResponse.getEntries()) {
            final InternalValue internalValue = JsonUtils.fromJsonString(entry.getBody(), InternalValue.class);
            if (internalValue == null) {
                continue;
            }

            try {
                BLOCKING_QUEUE.put(internalValue);
            } catch (InterruptedException e) {
                LOGGER.error("error while put to BLOCKING_QUEUE");
            }

            count++;

            if (count % INTERNAL_PAIR_COUNT == 0) {
                sendConcurrent(BLOCKING_QUEUE, round);
            }
        }

        sendConcurrent(BLOCKING_QUEUE, round);

        SeekTimeConfig.updateSeekTime();

        LOGGER.info("pull from db finish push, pushCost:{}ms, count:{}, seekTimestamp:{}, round:{}",
                System.currentTimeMillis() - start, count, seekTimestamp, round);
    }

    private void sendConcurrent(final BlockingQueue<InternalValue> blockingQueue, long round) {
        if (blockingQueue.size() == 0) {
            LOGGER.info("pull from db sendConcurrent start, return for no message to send, round:{}", round);
            return;
        }

        final long sendCount = blockingQueue.size();
        LOGGER.info("pull from db sendConcurrent start, send count:{}, round:{}", sendCount, round);
        final long start = System.currentTimeMillis();
        final CountDownLatch cdl = new CountDownLatch(blockingQueue.size());
        InternalValue internalValue;
        while ((internalValue = blockingQueue.poll()) != null) {
            final InternalValue immutableInternalValue = internalValue;
            pushThreadPool.execute(() -> {
                while (!send(
                        immutableInternalValue.getTopic(),
                        immutableInternalValue.getBody().getBytes(Charsets.UTF_8),
                        immutableInternalValue.getTags(),
                        immutableInternalValue.getProperties())) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(100);
                    } catch (InterruptedException e) {
                    }
                }
                cdl.countDown();
            });
        }

        try {
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        final long cost = System.currentTimeMillis() - start;
        LOGGER.info("pull from db sendConcurrent end, send count:{}, round:{}, cost:{}ms", sendCount, round, cost);
    }

    public void sendConcurrent(final BlockingQueue<InternalValue> blockingQueue, final String from) {
        if (blockingQueue.size() == 0) {
            LOGGER.info("pull from {} sendConcurrent start, return for no message to send", from);
            return;
        }

        final long sendCount = blockingQueue.size();
        LOGGER.info("pull from {} sendConcurrent start, send count:{}", from, sendCount);
        final long start = System.currentTimeMillis();
        final CountDownLatch cdl = new CountDownLatch(blockingQueue.size());
        InternalValue internalValue;
        while ((internalValue = blockingQueue.poll()) != null) {
            final InternalValue immutableInternalValue = internalValue;
            directPushThreadPool.execute(() -> {
                while (!send(
                        immutableInternalValue.getTopic(),
                        immutableInternalValue.getBody().getBytes(Charsets.UTF_8),
                        immutableInternalValue.getTags(),
                        immutableInternalValue.getProperties())) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(100);
                    } catch (InterruptedException e) {
                    }
                }
                cdl.countDown();
            });
        }

        try {
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        final long cost = System.currentTimeMillis() - start;
        LOGGER.info("pull from {} sendConcurrent end, send count:{}, cost:{}ms", from, sendCount, cost);
    }

    private boolean send(final String topic, final byte[] body, final String tags, final Map<String, String> properties) {
        Message msg = new Message(topic,
                tags,
                body);
        if (!msg.getProperties().equals(properties)) {
            MessageAccessor.setProperties(msg, properties);
        }
        // delete delay properties
        MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_DELAY_TIME);

        SendResult sendResult = null;
        try {
            sendResult = producer.send(msg);
        } catch (Exception e) {
            LOGGER.error("send msg error, msg is {}", msg, e);
        }

        if (sendResult != null) {
            return sendResult.getSendStatus() == SendStatus.SEND_OK;
        } else {
            return false;
        }

    }

    public void stop() {

        int times = 0;
        pushThreadPool.shutdown();
        while (!pushThreadPool.isShutdown()) {
            LOGGER.info("pushThreadPool is shutting down..., times={}", times);
            times++;
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                LOGGER.error("error while sleep for check pushThreadPool's shutdown, err:{}", e.getMessage(), e);
            }
        }
        LOGGER.info("pushThreadPool has shut down");

        times = 0;
        directPushThreadPool.shutdown();
        while (!directPushThreadPool.isShutdown()) {
            LOGGER.info("directPushThreadPool is shutting down..., times={}", times);
            times++;
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                LOGGER.error("error while sleep for check directPushThreadPool's shutdown, err:{}", e.getMessage(), e);
            }
        }
        LOGGER.info("directPushThreadPool has shut down");

        if (producer != null) {
            producer.shutdown();
            LOGGER.info("success shutdown producer");
        }
    }

    public static MqPushService getInstance() {
        if (instance == null) {
            synchronized (MqPushService.class) {
                if (instance == null) {
                    instance = new MqPushService();
                }
            }
        }
        return instance;
    }
}
