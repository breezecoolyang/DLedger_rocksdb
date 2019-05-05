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

package io.openmessaging.storage.dledger.cmdline;

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.Parameter;
import io.openmessaging.storage.dledger.client.DLedgerClient;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class PressureTestCommand extends BaseCommand {
    private static Logger logger = LoggerFactory.getLogger(AppendCommand.class);

    @Parameter(names = {"--group", "-g"}, description = "Group of this server")
    private String group = "default";

    @Parameter(names = {"--peers", "-p"}, description = "Peer info of this server")
    private String peers = "n0-localhost:20911";

    @Parameter(names = {"--time", "-t"}, description = "the timestamp to append")
    private long timestamp = 1536811267;

    @Override
    public void doCommand() {
        DLedgerClient dLedgerClient = new DLedgerClient(group, peers);
        dLedgerClient.startup();

        while (true) {
            long currentTimeMillis = System.currentTimeMillis();
            timestamp = currentTimeMillis / 1000;
            Random random = new Random();
            int randValue = random.nextInt(50);
            for (int i = 0; i < randValue; i++) {
                AppendEntryResponse response = dLedgerClient.append(timestamp, getRandomStr().getBytes());
                if (response.getIndex() < 0) {
                    logger.info("Append Error, Result:{}", JSON.toJSONString(response));
                }
                try {
                    Thread.sleep(random.nextInt(100));
                } catch (Exception e) {
                    //ignore this exception
                    logger.info("sleep Error", e);
                }
            }
        }
    }

    public String getRandomStr() {
        return RandomStringUtils.randomAlphanumeric(50);
    }
}
