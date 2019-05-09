package com.sunland.rocketmq.db;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.sunland.rocketmq.config.ConfigManager;
import com.sunland.rocketmq.config.SeekTimeConfig;
import com.sunland.rocketmq.model.InternalKey;
import com.sunland.rocketmq.utils.TsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;

public class Batcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(Batcher.class);

    private static final int PULL_BATCH_ITEM_NUM = ConfigManager.getConfig().getPullBatchNum();
//    private static final int MSG_BYTE_BASE_LEN = ConfigManager.getConfig().getPullConfig().getMsgByteBaseLen();

    private Multimap<Long, String> map = ArrayListMultimap.create();
    private volatile int itemNum = 0;
    private static volatile Batcher instance = null;
    private static DbOperation dbOperation = new DbOperation();
    private long lastCheckStamp = TsUtils.genTS();

    public static volatile ReentrantLock lock = new ReentrantLock();

    public static Batcher getInstance() {
        if (instance == null) {
            synchronized (Batcher.class) {
                instance = new Batcher();
            }
        }
        return instance;
    }

    private void checkFrequency() {
        long now = TsUtils.genTS();
        if (itemNum >= PULL_BATCH_ITEM_NUM || now - lastCheckStamp >= ConfigManager.getConfig().getFlushCheckInterval()) {
            flush();
        }
        lastCheckStamp = now;

    }

    public void flush() {
        lock.lock();
        try {
            if (itemNum > 0) {
                dbOperation.append(map);
            }
            itemNum = map.size();
        } finally
        {
            lock.unlock();
        }

    }

    private void put(final Long key, final String value) {
        lock.lock();
        try {
            int len = 0;
            if (value != null) {
                len = value.length();
            }

            map.put(key, value);
            LOGGER.info("put to cf, timestamp:{}, len:{}", key, len);
            itemNum++;
            checkFrequency();

        } finally {
            lock.unlock();
        }
    }

    public boolean checkAndPut(final InternalKey key, final String strVal) {
        lock.lock();
        try {
            if (key.getTimestamp() > SeekTimeConfig.getSeekTime()) {
                put(key.getTimestamp(), strVal);
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    public static void close() {
        dbOperation.shutdown();
    }
}
