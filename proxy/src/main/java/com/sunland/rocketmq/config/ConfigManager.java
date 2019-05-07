package com.sunland.rocketmq.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigManager.class);

    private static ScheduleConfig cfg = null;

    public static void initConfig(final String configPath) {
        try {
            final long start = System.currentTimeMillis();
            //cfg = ConfigUtils.newConfig(configPath, ChronosConfig.class);
            final long cost = System.currentTimeMillis() - start;
            LOGGER.info("succ init chronos config, cost:{}ms, config:{}, configFilePath:{}", cost, cfg, configPath);
        } catch (Exception e) {
            LOGGER.error("error initConfig, configPath:{}, err:{}", configPath, e.getMessage(), e);
        }
    }

    public static ScheduleConfig getConfig() {
        return cfg;
    }






}
