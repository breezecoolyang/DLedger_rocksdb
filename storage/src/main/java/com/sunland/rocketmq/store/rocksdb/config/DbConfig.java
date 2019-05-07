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

package com.sunland.rocketmq.store.rocksdb.config;


public class DbConfig implements ConfigurationValidator {

    private String dbPath;
    private String seekTimestampPath;
    private String dbPathBackup;
    private String dbPathRestore;
    private int maxBackgroundFlushes;
    private int maxBackgroundCompactions;
    private int writeBufferSize;
    private int maxWriteBufferNumber;
    private int maxSubcompactions;
    private int level0SlowdownWritesTrigger;
    private int level0StopWritesTrigger;
    private int targetFileSizeBase;
    private int maxBytesForLevelBase;
    private int delayedWriteRate;
    private int baseBackgroundCompactions;

    public DbConfig() {
        dbPath = "/home/fengqing/schedule-storage/rocksdb";
        seekTimestampPath = "/home/fengqing/schedule-storage/seektimestamp";
        dbPathBackup = "/home/fengqing/schedule-storage/rocksdb_backup";
        dbPathRestore = "/home/fengqing/schedule-storage/rocksdb_restore";
        maxBackgroundFlushes = 10;
        maxBackgroundCompactions = 35;
        baseBackgroundCompactions = 5;
        writeBufferSize = 128;
        maxWriteBufferNumber = 10;
        maxSubcompactions = 10;
        level0SlowdownWritesTrigger =  30;
        level0StopWritesTrigger = 50;
        maxBytesForLevelBase = 512;
        targetFileSizeBase = 128;
        delayedWriteRate = 64;

    }

    public String getDbPath() {
        return dbPath;
    }

    public void setDbPath(String dbPath) {
        this.dbPath = dbPath;
    }

    public String getSeekTimestampPath() {
        return seekTimestampPath;
    }

    public void setSeekTimestampPath(String seekTimestampPath) {
        this.seekTimestampPath = seekTimestampPath;
    }

    public String getDbPathBackup() {
        return dbPathBackup;
    }

    public void setDbPathBackup(String dbPathBackup) {
        this.dbPathBackup = dbPathBackup;
    }

    public String getDbPathRestore() {
        return dbPathRestore;
    }

    public void setDbPathRestore(String dbPathRestore) {
        this.dbPathRestore = dbPathRestore;
    }

    public int getMaxBackgroundFlushes() {
        return maxBackgroundFlushes;
    }

    public void setMaxBackgroundFlushes(int maxBackgroundFlushes) {
        this.maxBackgroundFlushes = maxBackgroundFlushes;
    }

    public int getMaxBackgroundCompactions() {
        return maxBackgroundCompactions;
    }

    public void setMaxBackgroundCompactions(int maxBackgroundCompactions) {
        this.maxBackgroundCompactions = maxBackgroundCompactions;
    }

    public int getWriteBufferSize() {
        return writeBufferSize;
    }

    public void setWriteBufferSize(int writeBufferSize) {
        this.writeBufferSize = writeBufferSize;
    }

    public int getMaxWriteBufferNumber() {
        return maxWriteBufferNumber;
    }

    public void setMaxWriteBufferNumber(int maxWriteBufferNumber) {
        this.maxWriteBufferNumber = maxWriteBufferNumber;
    }

    public int getMaxSubcompactions() {
        return maxSubcompactions;
    }

    public void setMaxSubcompactions(int maxSubcompactions) {
        this.maxSubcompactions = maxSubcompactions;
    }

    public int getLevel0SlowdownWritesTrigger() {
        return level0SlowdownWritesTrigger;
    }

    public void setLevel0SlowdownWritesTrigger(int level0SlowdownWritesTrigger) {
        this.level0SlowdownWritesTrigger = level0SlowdownWritesTrigger;
    }

    public int getLevel0StopWritesTrigger() {
        return level0StopWritesTrigger;
    }

    public void setLevel0StopWritesTrigger(int level0StopWritesTrigger) {
        this.level0StopWritesTrigger = level0StopWritesTrigger;
    }

    public int getTargetFileSizeBase() {
        return targetFileSizeBase;
    }

    public void setTargetFileSizeBase(int targetFileSizeBase) {
        this.targetFileSizeBase = targetFileSizeBase;
    }

    public int getMaxBytesForLevelBase() {
        return maxBytesForLevelBase;
    }

    public void setMaxBytesForLevelBase(int maxBytesForLevelBase) {
        this.maxBytesForLevelBase = maxBytesForLevelBase;
    }

    public int getDelayedWriteRate() {
        return delayedWriteRate;
    }

    public void setDelayedWriteRate(int delayedWriteRate) {
        this.delayedWriteRate = delayedWriteRate;
    }

    public int getBaseBackgroundCompactions() {
        return baseBackgroundCompactions;
    }

    public void setBaseBackgroundCompactions(int baseBackgroundCompactions) {
        this.baseBackgroundCompactions = baseBackgroundCompactions;
    }

    @Override
    public boolean validate() {
        return true;
    }

    @Override
    public String toString() {
        return "DbConfig{" +
                "dbPath='" + dbPath + '\'' +
                ", seekTimestampPath='" + seekTimestampPath + '\'' +
                ", dbPathBackup='" + dbPathBackup + '\'' +
                ", dbPathRestore='" + dbPathRestore + '\'' +
                ", maxBackgroundFlushes=" + maxBackgroundFlushes +
                ", maxBackgroundCompactions=" + maxBackgroundCompactions +
                ", writeBufferSize=" + writeBufferSize +
                ", maxWriteBufferNumber=" + maxWriteBufferNumber +
                ", maxSubcompactions=" + maxSubcompactions +
                ", level0SlowdownWritesTrigger=" + level0SlowdownWritesTrigger +
                ", level0StopWritesTrigger=" + level0StopWritesTrigger +
                ", targetFileSizeBase=" + targetFileSizeBase +
                ", maxBytesForLevelBase=" + maxBytesForLevelBase +
                ", delayedWriteRate=" + delayedWriteRate +
                ", baseBackgroundCompactions=" + baseBackgroundCompactions +
                '}';
    }
}
