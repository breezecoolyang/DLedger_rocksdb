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

package io.openmessaging.storage.dledger.store.rocksdb.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigManager.class);

    private static ChronosConfig cfg = null;

    public static void initConfig() {
        try {
            final long start = System.currentTimeMillis();
            cfg = new ChronosConfig();
            final long cost = System.currentTimeMillis() - start;
            LOGGER.info("succ init chronos config, cost:{}ms, config:{}", cost, cfg);
        } catch (Exception e) {
            LOGGER.error("error initConfig, err:{}", e.getMessage(), e);
        }
    }

    public static ChronosConfig getConfig() {
        return cfg;
    }
}
