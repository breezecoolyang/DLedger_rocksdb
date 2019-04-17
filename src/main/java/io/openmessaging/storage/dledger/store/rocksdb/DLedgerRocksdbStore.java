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

package io.openmessaging.storage.dledger.store.rocksdb;

import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.store.DLedgerMemoryStore;
import io.openmessaging.storage.dledger.store.DLedgerStore;
import io.openmessaging.storage.dledger.store.rocksdb.config.ConfigManager;
import io.openmessaging.storage.dledger.store.rocksdb.db.CFManager;
import io.openmessaging.storage.dledger.store.rocksdb.db.RDB;
import io.openmessaging.storage.dledger.utils.PreConditions;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DLedgerRocksdbStore extends DLedgerStore {

    private static Logger logger = LoggerFactory.getLogger(DLedgerMemoryStore.class);

    private long ledgerBeginIndex = -1;
    private long ledgerEndIndex = -1;
    private long committedIndex = -1;
    private long ledgerEndTerm;
    private static ColumnFamilyHandle cfHandle;
    WriteBatch wb;
    private DLedgerConfig dLedgerConfig;
    private MemberState memberState;

    public DLedgerRocksdbStore(DLedgerConfig dLedgerConfig, MemberState memberState) {
        this.dLedgerConfig = dLedgerConfig;
        this.memberState = memberState;
    }


    public void startup() {
        ConfigManager.initConfig();
        RDB.init(ConfigManager.getConfig().getDbConfig().getDbPath());
        cfHandle = CFManager.cfhDefault;
        wb = new WriteBatch();
    }

    public void shutdown() {
        RDB.close();
    }

    @Override
    public DLedgerEntry appendAsLeader(DLedgerEntry entry) {
        PreConditions.check(memberState.isLeader(), DLedgerResponseCode.NOT_LEADER);
        synchronized (memberState) {
            PreConditions.check(memberState.isLeader(), DLedgerResponseCode.NOT_LEADER);
            ledgerEndIndex = getKey(entry);
            committedIndex = ledgerEndIndex;
            ledgerEndTerm = memberState.currTerm();
            entry.setIndex(ledgerEndIndex);
            entry.setTerm(memberState.currTerm());
            writeData(entry);
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] Append as Leader {} {}", memberState.getSelfId(), entry.getIndex(), entry.getBody().length);
            }
            if (ledgerBeginIndex == -1) {
                ledgerBeginIndex = ledgerEndIndex;
            }
            updateLedgerEndIndexAndTerm();
            return entry;
        }
    }

    public long getKey(DLedgerEntry entry) {
        String kv = new String(entry.getBody());
        String[] splitValue = kv.split("@");
        return Long.parseLong(splitValue[0]);

    }

    public String getValue(DLedgerEntry entry) {
        String kv = new String(entry.getBody());
        String[] splitValue = kv.split("@");

        return splitValue[1];

    }

    public byte[] composeData(String key, String value) {
        String result = key + value;
        return result.getBytes();
    }

    public void writeData(DLedgerEntry entry) {

        String kv = new String(entry.getBody());
        String[] splitValue = kv.split("@");
        String key = splitValue[0];
        String value = splitValue[1];

        logger.error("write value to db key is {}, value is {}", key, value);
        wb.put(cfHandle, key.getBytes(), value.getBytes());
        RDB.writeAsync(wb);
        wb.clear();

    }

    @Override
    public DLedgerEntry get(Long index) {
        RocksIterator it = RDB.newIterator(cfHandle);
        DLedgerEntry dLedgerEntry = new DLedgerEntry();
        byte[] now = index.toString().getBytes();
        for (it.seek(now); it.isValid(); it.next()) {
            logger.error("key is {}, value is {}", it.key(), it.value());
            dLedgerEntry.setBody(composeData(new String(it.key()), new String(it.value())));
        }

        return dLedgerEntry;
    }

    @Override
    public long getLedgerBeginIndex() {
        return ledgerBeginIndex;
    }

    @Override
    public long truncate(DLedgerEntry entry, long leaderTerm, String leaderId) {
        return appendAsFollower(entry, leaderTerm, leaderId).getIndex();
    }

    @Override
    public DLedgerEntry appendAsFollower(DLedgerEntry entry, long leaderTerm, String leaderId) {
        PreConditions.check(memberState.isFollower(), DLedgerResponseCode.NOT_FOLLOWER);
        synchronized (memberState) {
            PreConditions.check(memberState.isFollower(), DLedgerResponseCode.NOT_FOLLOWER);
            PreConditions.check(leaderTerm == memberState.currTerm(), DLedgerResponseCode.INCONSISTENT_TERM);
            PreConditions.check(leaderId.equals(memberState.getLeaderId()), DLedgerResponseCode.INCONSISTENT_LEADER);
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] Append as Follower {} {}", memberState.getSelfId(), entry.getIndex(), entry.getBody().length);
            }
            ledgerEndTerm = memberState.currTerm();
            ledgerEndIndex = entry.getIndex();
            committedIndex = entry.getIndex();
            writeData(entry);
            if (ledgerBeginIndex == -1) {
                ledgerBeginIndex = ledgerEndIndex;
            }
            updateLedgerEndIndexAndTerm();
            return entry;
        }
    }

    public long getCommittedIndex() {
        return committedIndex;
    }

    public long getLedgerEndTerm() {
        return ledgerEndTerm;
    }

    public long getLedgerEndIndex() {
        return ledgerEndIndex;
    }

    public void updateCommittedIndex(long term, long newCommittedIndex) {
        if (newCommittedIndex == -1
                || ledgerEndIndex == -1
                || term < memberState.currTerm()
                || newCommittedIndex == this.committedIndex) {
            return;
        }
        if (newCommittedIndex < this.committedIndex
                || newCommittedIndex < this.ledgerBeginIndex) {
            logger.warn("[MONITOR]Skip update committed index for new={} < old={} or new={} < beginIndex={}", newCommittedIndex, this.committedIndex, newCommittedIndex, this.ledgerBeginIndex);
            return;
        }
        long endIndex = ledgerEndIndex;
        if (newCommittedIndex > endIndex) {
            //If the node fall behind too much, the committedIndex will be larger than enIndex.
            newCommittedIndex = endIndex;
        }
        DLedgerEntry dLedgerEntry = get(newCommittedIndex);
        PreConditions.check(dLedgerEntry != null, DLedgerResponseCode.DISK_ERROR);
        this.committedIndex = newCommittedIndex;
    }
}
