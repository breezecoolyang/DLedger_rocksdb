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

package com.sunland.rocketmq.client;

import com.sunland.rocketmq.ShutdownAbleThread;
import com.sunland.rocketmq.utils.DLedgerUtils;
import com.sunland.rocketmq.protocol.AppendEntryRequest;
import com.sunland.rocketmq.protocol.AppendEntryResponse;
import com.sunland.rocketmq.protocol.DLedgerResponseCode;
import com.sunland.rocketmq.protocol.GetEntriesRequest;
import com.sunland.rocketmq.protocol.GetEntriesResponse;
import com.sunland.rocketmq.protocol.MetadataRequest;
import com.sunland.rocketmq.protocol.MetadataResponse;
import com.sunland.rocketmq.protocol.GetListEntriesRequest;
import com.sunland.rocketmq.protocol.GetListEntriesResponse;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLedgerClient {

    private static Logger logger = LoggerFactory.getLogger(DLedgerClient.class);
    private final Map<String, String> peerMap = new ConcurrentHashMap<>();
    private final String group;
    private String leaderId;
    private DLedgerClientRpcService dLedgerClientRpcService;

    private MetadataUpdater metadataUpdater = new MetadataUpdater("MetadataUpdater", logger);

    public DLedgerClient(String group, String peers) {
        this.group = group;
        updatePeers(peers);
        dLedgerClientRpcService = new DLedgerClientRpcNettyService();
        dLedgerClientRpcService.updatePeers(peers);
        leaderId = peerMap.keySet().iterator().next();
    }

    public AppendEntryResponse append(byte[] body) {
        try {
            waitOnUpdatingMetadata(1500, false);
            if (leaderId == null) {
                AppendEntryResponse appendEntryResponse = new AppendEntryResponse();
                appendEntryResponse.setCode(DLedgerResponseCode.METADATA_ERROR.getCode());
                return appendEntryResponse;
            }
            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setGroup(group);
            appendEntryRequest.setRemoteId(leaderId);
            appendEntryRequest.setBody(body);
            AppendEntryResponse response = dLedgerClientRpcService.append(appendEntryRequest).get();
            if (response.getCode() == DLedgerResponseCode.NOT_LEADER.getCode()) {
                waitOnUpdatingMetadata(1500, true);
                if (leaderId != null) {
                    appendEntryRequest.setRemoteId(leaderId);
                    response = dLedgerClientRpcService.append(appendEntryRequest).get();
                }
            }
            return response;
        } catch (Exception e) {
            needFreshMetadata();
            logger.error("{}", e);
            AppendEntryResponse appendEntryResponse = new AppendEntryResponse();
            appendEntryResponse.setCode(DLedgerResponseCode.INTERNAL_ERROR.getCode());
            return appendEntryResponse;
        }
    }

    public AppendEntryResponse append(long timestamp, byte[] body) {
        try {
            waitOnUpdatingMetadata(1500, false);
            if (leaderId == null) {
                AppendEntryResponse appendEntryResponse = new AppendEntryResponse();
                appendEntryResponse.setCode(DLedgerResponseCode.METADATA_ERROR.getCode());
                return appendEntryResponse;
            }
            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setGroup(group);
            appendEntryRequest.setRemoteId(leaderId);
            appendEntryRequest.setBody(body);
            appendEntryRequest.setTimestamp(timestamp);
            AppendEntryResponse response = dLedgerClientRpcService.append(appendEntryRequest).get();
            if (response.getCode() == DLedgerResponseCode.NOT_LEADER.getCode()) {
                waitOnUpdatingMetadata(1500, true);
                if (leaderId != null) {
                    appendEntryRequest.setRemoteId(leaderId);
                    response = dLedgerClientRpcService.append(appendEntryRequest).get();
                }
            }
            return response;
        } catch (Exception e) {
            needFreshMetadata();
            logger.error("{}", e);
            AppendEntryResponse appendEntryResponse = new AppendEntryResponse();
            appendEntryResponse.setCode(DLedgerResponseCode.INTERNAL_ERROR.getCode());
            return appendEntryResponse;
        }
    }

    public GetEntriesResponse get(long index) {
        try {
            waitOnUpdatingMetadata(1500, false);
            if (leaderId == null) {
                GetEntriesResponse response = new GetEntriesResponse();
                response.setCode(DLedgerResponseCode.METADATA_ERROR.getCode());
                return response;
            }

            GetEntriesRequest request = new GetEntriesRequest();
            request.setGroup(group);
            request.setRemoteId(leaderId);
            request.setBeginIndex(index);
            GetEntriesResponse response = dLedgerClientRpcService.get(request).get();
            if (response.getCode() == DLedgerResponseCode.NOT_LEADER.getCode()) {
                waitOnUpdatingMetadata(1500, true);
                if (leaderId != null) {
                    request.setRemoteId(leaderId);
                    response = dLedgerClientRpcService.get(request).get();
                }
            }
            return response;
        } catch (Exception t) {
            needFreshMetadata();
            logger.error("", t);
            GetEntriesResponse getEntriesResponse = new GetEntriesResponse();
            getEntriesResponse.setCode(DLedgerResponseCode.INTERNAL_ERROR.getCode());
            return getEntriesResponse;
        }
    }

    public GetListEntriesResponse getByTime(long timestamp) {
        try {
            waitOnUpdatingMetadata(1500, false);
            if (leaderId == null) {
                GetListEntriesResponse response = new GetListEntriesResponse();
                response.setCode(DLedgerResponseCode.METADATA_ERROR.getCode());
                return response;
            }

            GetListEntriesRequest request = new GetListEntriesRequest();
            request.setGroup(group);
            request.setRemoteId(leaderId);
            request.setTimestamp(timestamp);
            GetListEntriesResponse response = dLedgerClientRpcService.getByTime(request).get();
            if (response.getCode() == DLedgerResponseCode.NOT_LEADER.getCode()) {
                waitOnUpdatingMetadata(1500, true);
                if (leaderId != null) {
                    request.setRemoteId(leaderId);
                    response = dLedgerClientRpcService.getByTime(request).get();
                }
            }
            return response;
        } catch (Exception t) {
            needFreshMetadata();
            logger.error("getByTime error ", t);
            GetListEntriesResponse getListEntriesResponse = new GetListEntriesResponse();
            getListEntriesResponse.setCode(DLedgerResponseCode.INTERNAL_ERROR.getCode());
            return getListEntriesResponse;
        }
    }



    public void startup() {
        this.dLedgerClientRpcService.startup();
        this.metadataUpdater.start();
    }

    public void shutdown() {
        this.dLedgerClientRpcService.shutdown();
        this.metadataUpdater.shutdown();
    }

    private void updatePeers(String peers) {
        for (String peerInfo : peers.split(";")) {
            peerMap.put(peerInfo.split("-")[0], peerInfo.split("-")[1]);
        }
    }

    private synchronized void needFreshMetadata() {
        leaderId = null;
        metadataUpdater.wakeup();
    }

    private synchronized void waitOnUpdatingMetadata(long maxWaitMs, boolean needFresh) {
        if (needFresh) {
            leaderId = null;
        } else if (leaderId != null) {
            return;
        }
        long start = System.currentTimeMillis();
        while (DLedgerUtils.elapsed(start) < maxWaitMs && leaderId == null) {
            metadataUpdater.wakeup();
            try {
                wait(1000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private class MetadataUpdater extends ShutdownAbleThread {

        public MetadataUpdater(String name, Logger logger) {
            super(name, logger);
        }

        private void getMetadata(String peerId, boolean isLeader) {
            try {
                MetadataRequest request = new MetadataRequest();
                request.setGroup(group);
                request.setRemoteId(peerId);
                CompletableFuture<MetadataResponse> future = dLedgerClientRpcService.metadata(request);
                MetadataResponse response = future.get(1500, TimeUnit.MILLISECONDS);
                if (response.getLeaderId() != null) {
                    leaderId = response.getLeaderId();
                    if (response.getPeers() != null) {
                        peerMap.putAll(response.getPeers());
                        dLedgerClientRpcService.updatePeers(response.getPeers());
                    }
                }
            } catch (Throwable t) {
                if (isLeader) {
                    needFreshMetadata();
                }
                logger.warn("Get metadata failed from {}", peerId, t);
            }

        }

        @Override public void doWork() {
            try {
                if (leaderId == null) {
                    for (String peer : peerMap.keySet()) {
                        getMetadata(peer, false);
                        if (leaderId != null) {
                            synchronized (DLedgerClient.this) {
                                DLedgerClient.this.notifyAll();
                            }
                            DLedgerUtils.sleep(1000);
                            break;
                        }
                    }
                } else {
                    getMetadata(leaderId, true);
                }
                waitForRunning(3000);
            } catch (Throwable t) {
                logger.error("Error", t);
                DLedgerUtils.sleep(1000);
            }
        }
    }

}
