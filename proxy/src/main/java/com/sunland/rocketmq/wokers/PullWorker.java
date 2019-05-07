package com.sunland.rocketmq.wokers;

import com.sunland.rocketmq.services.MqPullService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class PullWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger(PullWorker.class);


    private static final List<MqPullService> PULL_SERVICES = new ArrayList<>();

    private static int threadNum = 4;
    private static volatile PullWorker instance = null;

    private PullWorker() {
    }

    public void start() {
        LOGGER.info("PullWorker will start ...");
        final long start = System.currentTimeMillis();


        for (int i = 0; i < threadNum; i++) {
            MqPullService mqPullService = new MqPullService(i);
            PULL_SERVICES.add(mqPullService);
            mqPullService.start();
        }

        LOGGER.info("PullWorker has started, cost:{}ms", System.currentTimeMillis() - start);
    }

    public void stop() {
        LOGGER.info("PullWorker will stop ...");
        final long start = System.currentTimeMillis();

        PULL_SERVICES.forEach(MqPullService::stop);

        LOGGER.info("PullWorker has stopped, cost:{}ms", System.currentTimeMillis() - start);
    }

    public static PullWorker getInstance() {
        if (instance == null) {
            synchronized (PullWorker.class) {
                if (instance == null) {
                    instance = new PullWorker();
                }
            }
        }
        return instance;
    }
}
