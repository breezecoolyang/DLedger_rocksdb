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

package com.sunland.rocketmq.store;

import com.sunland.rocketmq.MemberState;
import com.sunland.rocketmq.entry.DLedgerEntry;

public abstract class DLedgerStore {

    public MemberState getMemberState() {
        return null;
    }

    public abstract DLedgerEntry appendAsLeader(DLedgerEntry entry);

    public abstract DLedgerEntry appendAsFollower(DLedgerEntry entry, long leaderTerm, String leaderId);

    public abstract DLedgerEntry get(Long index);

    public abstract long getCommittedIndex();

    public void updateCommittedIndex(long term, long committedIndex) {

    }

    public abstract long getLedgerEndTerm();

    public abstract long getLedgerEndIndex();

    public abstract long getLedgerBeginIndex();

    protected void updateLedgerEndIndexAndTerm() {
        if (getMemberState() != null) {
            getMemberState().updateLedgerIndexAndTerm(getLedgerEndIndex(), getLedgerEndTerm());
        }
    }

    public void flush() {

    }

    public long truncate(DLedgerEntry entry, long leaderTerm, String leaderId) {
        return -1;
    }

    public void startup() {

    }

    public void shutdown() {

    }
}
