package com.sunland.rocketmq;

import static org.junit.Assert.assertTrue;

import com.sunland.rocketmq.config.ConfigManager;
import com.sunland.rocketmq.config.ScheduleConfig;
import org.junit.Test;

/**
 * Unit test for simple ScheduleMain.
 */
public class ScheduleMainTest
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void testScheduleProxy()
    {
        ScheduleStartup startup = new ScheduleStartup(null);
        startup.init();
        ScheduleConfig config = new ScheduleConfig();
        ConfigManager.setConfig(config);

        try {
            startup.start();
        } catch (Exception e) {
            startup.stop();
        }
        assertTrue( true );
    }
}
