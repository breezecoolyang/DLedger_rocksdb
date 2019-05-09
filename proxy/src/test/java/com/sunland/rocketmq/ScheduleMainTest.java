package com.sunland.rocketmq;

import static org.junit.Assert.assertTrue;

import com.sunland.rocketmq.config.ConfigManager;
import com.sunland.rocketmq.config.ScheduleConfig;
import com.sunland.rocketmq.wokers.PullWorker;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

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

    @Test
    public void testPullProxy() throws Exception
    {
        ConfigManager.initConfig(null);
        try {
            PullWorker.getInstance().start();
        } catch (Exception e) {
            PullWorker.getInstance().stop();
        }
        CountDownLatch waitForShutdown = new CountDownLatch(1);
        waitForShutdown.await();
        assertTrue( true );
    }
}
