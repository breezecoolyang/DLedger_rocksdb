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
        }, 3000, ConfigManager.getConfig().getFlushSeekTimeInterval());

        log.error("seek time init success");
    }

    public static long getSeekTime() {
        return seekTimestamp;
    }

    public static void setSeekTime(long seekTime) { seekTimestamp = seekTime;}

    public static void updateSeekTime() {
        /*the lock is to make sure we only update time stamp  after the msg have been written to storage device
         * consider a situation as follows
         *            pull                               push
         *                                           timestamp ---> 5
         *        msg timestamp 6 (can write (6>5))
         *                                           7 (if the timestamp update to 7 when we write msg, the msg with timestamp 6 will be missed)
         *
         */
        Batcher.lock.lock();
        try {
            seekTimestamp++;
        } finally {
            Batcher.lock.unlock();
        }
    }

}
