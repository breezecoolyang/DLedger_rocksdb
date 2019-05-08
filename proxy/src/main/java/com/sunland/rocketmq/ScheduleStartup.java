package com.sunland.rocketmq;

import com.sunland.rocketmq.config.ConfigManager;
import com.sunland.rocketmq.wokers.PullWorker;
import com.sunland.rocketmq.wokers.PushWorker;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ScheduleStartup {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScheduleStartup.class);

    private CountDownLatch waitForShutdown;
    private String configFilePath = "";
    private PullWorker pullWorker;
    private PushWorker pushWorker;


    ScheduleStartup(final String configFilePath) {
        if (StringUtils.isNotBlank(configFilePath)) {
            this.configFilePath = configFilePath;
        }

    }

    public void init() {
        /* init config */
        ConfigManager.initConfig(configFilePath);
    }

    public void start() throws Exception {
        LOGGER.info("start to launch ScheduleStartup...");
        final long start = System.currentTimeMillis();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    LOGGER.info("start to stop ScheduleStartup...");
                    final long start = System.currentTimeMillis();
                    ScheduleStartup.this.stop();
                    final long cost = System.currentTimeMillis() - start;
                    LOGGER.info("success stop ScheduleStartup, cost:{}ms", cost);
                } catch (Exception e) {
                    LOGGER.error("error while shutdown ScheduleStartup, err:{}", e.getMessage(), e);
                } finally {

                }
            }
        });

        waitForShutdown = new CountDownLatch(1);

        /* init pull worker */
        pullWorker = PullWorker.getInstance();
        pullWorker.start();

        /* init push worker */
        pushWorker = PushWorker.getInstance();
        pushWorker.start();

        final long cost = System.currentTimeMillis() - start;
        LOGGER.info("success start ScheduleStartup, cost:{}ms", cost);

        waitForShutdown.await();

    }

    void stop() {
        /* stop pull from MQ */
        if (pullWorker != null) {
            pullWorker.stop();
        }

        /* stop push to MQ */
        if (pushWorker != null) {
            pushWorker.stop();
        }

        if (waitForShutdown != null) {
            waitForShutdown.countDown();
            waitForShutdown = null;
        }
    }
}
