package com.sunland.rocketmq.config;

import com.sunland.rocketmq.db.Batcher;
import com.sunland.rocketmq.utils.IOUtils;
import com.sunland.rocketmq.utils.TsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

public class SeekTimeConfig {

    private static final Logger log = LoggerFactory.getLogger(SeekTimeConfig.class);
    private static final String SEEK_TIME_FILE = "seekTime";
    private static final String SEEK_TIME = "seekTime";
    private static volatile long seekTimestamp = -1;
    private static Properties properties;
    private static final Timer TIMER = new Timer("ScheduleMessageTimerThread", true);

    public static void persistSeekTime() {
        try {
            properties.put(SEEK_TIME, seekTimestamp);
            String data = IOUtils.properties2String(properties);
            IOUtils.string2File(data, ConfigManager.getConfig().getSeekTimePath() + File.separator + SEEK_TIME_FILE);
        } catch (Throwable t) {
            log.error("Persist seekTime failed", t);
        }
    }

    public static void loadSeekTime() {
        try {
            if (seekTimestamp == -1) {
                seekTimestamp = TsUtils.genTS();
            }

            String data = IOUtils.file2String(ConfigManager.getConfig().getSeekTimePath() + File.separator + SEEK_TIME_FILE);
            Properties prop = IOUtils.string2Properties(data);
            if (prop != null) {
                properties = prop;
                seekTimestamp = Long.valueOf(String.valueOf(properties.get(SEEK_TIME)).trim());
                return;
            }
        } catch (Throwable t) {
            log.error("Load seekTime failed", t);
        }
        if (properties == null) {
            properties = new Properties();
            properties.put(SEEK_TIME, Long.valueOf(seekTimestamp).toString());
        }


        TIMER.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                try {
                    persistSeekTime();
                } catch (Throwable e) {
                    log.error("scheduleAtFixedRate flush exception", e);
                }
            }
        }, 10000, ConfigManager.getConfig().getFlushSeekTimeInterval());
    }

    public static long getSeekTime() {
        return seekTimestamp;
    }

    public static void updateSeekTime() {
        Batcher.lock.lock();
        try {
            seekTimestamp++;
        } finally {
            Batcher.lock.unlock();
        }
    }

}
