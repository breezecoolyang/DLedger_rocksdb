package com.sunland.rocketmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class ScheduleMain {
    private static final  Logger LOGGER = LoggerFactory.getLogger(ScheduleMain.class);

    public static void main(String[] args) {
        Thread.setDefaultUncaughtExceptionHandler((thread, exception) ->
                LOGGER.error("UncaughtException in Thread " + thread.toString(), exception));

        if (args.length < 1) {
            LOGGER.error("params error!");
            return;
        }

        System.out.printf("args[0] is %s", args[0]);
        ScheduleStartup startup = new ScheduleStartup(args[0]);
        try {
            startup.start();
        } catch (Exception e) {
            LOGGER.error("error while start chronos, err:{}", e.getMessage(), e);
            startup.stop();
        }
    }
}
