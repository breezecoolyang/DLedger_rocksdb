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

package com.sunland.rocketmq.protocol;

import static com.sunland.rocketmq.protocol.VoteResponse.RESULT.UNKNOWN;

public class VoteResponse extends RequestOrResponse {

    public RESULT voteResult = UNKNOWN;

    public VoteResponse() {

    }

    public VoteResponse(VoteRequest request) {
        copyBaseInfo(request);
    }

    public RESULT getVoteResult() {
        return voteResult;
    }

    public void setVoteResult(RESULT voteResult) {
        this.voteResult = voteResult;
    }

    public VoteResponse voteResult(RESULT voteResult) {
        this.voteResult = voteResult;
        return this;
    }

    public VoteResponse term(long term) {
        this.term = term;
        return this;
    }

    public enum RESULT {
        UNKNOWN,
        ACCEPT,
        REJECT_UNKNOWN_LEADER,
        REJECT_UNEXPECTED_LEADER,
        REJECT_EXPIRED_VOTE_TERM,
        REJECT_ALREADY_VOTED,
        REJECT_ALREADY__HAS_LEADER,
        REJECT_TERM_NOT_READY,
        REJECT_TERM_SMALL_THAN_LEDGER,
        REJECT_EXPIRED_LEDGER_TERM,
        REJECT_SMALL_LEDGER_END_INDEX;
    }

    public enum ParseResult {
        WAIT_TO_REVOTE,
        REVOTE_IMMEDIATELY,
        PASSED,
        WAIT_TO_VOTE_NEXT;
    }
}
