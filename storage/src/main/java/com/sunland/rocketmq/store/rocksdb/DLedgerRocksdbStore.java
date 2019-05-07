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

package com.sunland.rocketmq.store.rocksdb;

import com.sunland.rocketmq.entry.DLedgerRocksdbEntryCoder;
import com.sunland.rocketmq.protocol.DLedgerResponseCode;
import com.sunland.rocketmq.store.rocksdb.config.ConfigManager;
import com.sunland.rocketmq.store.rocksdb.db.RDB;
import com.sunland.rocketmq.DLedgerConfig;
import com.sunland.rocketmq.MemberState;
import com.sunland.rocketmq.ShutdownAbleThread;
import com.sunland.rocketmq.entry.DLedgerEntry;
import com.sunland.rocketmq.store.DLedgerStore;
import com.sunland.rocketmq.store.file.MmapFile;
import com.sunland.rocketmq.store.file.MmapFileList;
import com.sunland.rocketmq.store.file.SelectMmapBufferResult;
import com.sunland.rocketmq.utils.DLedgerUtils;
import com.sunland.rocketmq.utils.IOUtils;
import com.sunland.rocketmq.utils.PreConditions;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;


public class DLedgerRocksdbStore extends DLedgerStore {

    private static Logger logger = LoggerFactory.getLogger(DLedgerRocksdbStore.class);

    public static final String CHECK_POINT_FILE = "checkpoint";
    public static final String END_INDEX_KEY = "endIndex";
    public static final String COMMITTED_INDEX_KEY = "committedIndex";
    public static final int MAGIC_1 = 1;
    public static final int CURRENT_MAGIC = MAGIC_1;
    public RDB rdb;
    public static final int INDEX_UNIT_SIZE = 28; //magic + index + key_prefix(timestamp + index)
    private static final String KEY_SEPARATOR = "_";

    private long ledgerBeginIndex = -1;
    private long ledgerEndIndex = -1;
    private long committedIndex = -1;
    private long ledgerEndTerm;
    private ConfigManager configManager;
    private WriteBatch wb;
    private DLedgerConfig dLedgerConfig;
    private MemberState memberState;
    private MmapFileList indexFileList;
    private ThreadLocal<ByteBuffer> localIndexBuffer;
    private DLedgerRocksdbStore.FlushDataService flushDataService;
    private DLedgerRocksdbStore.CleanSpaceService cleanSpaceService;

    private boolean isDiskFull = false;
    private long lastCheckPointTimeMs = System.currentTimeMillis();
    private AtomicBoolean hasLoaded = new AtomicBoolean(false);
    private AtomicBoolean hasRecovered = new AtomicBoolean(false);

    public DLedgerRocksdbStore(DLedgerConfig dLedgerConfig, MemberState memberState) {
        this.dLedgerConfig = dLedgerConfig;
        this.memberState = memberState;
        this.indexFileList = new MmapFileList(dLedgerConfig.getIndexStorePath(), dLedgerConfig.getMappedFileSizeForRocksDBEntryIndex());
        localIndexBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(INDEX_UNIT_SIZE * 2));
        flushDataService = new DLedgerRocksdbStore.FlushDataService("DLedgerFlushDataService", logger);
        cleanSpaceService = new DLedgerRocksdbStore.CleanSpaceService("DLedgerCleanSpaceService", logger);
        configManager = new ConfigManager();
        configManager.initConfig();
        configManager.getConfig().getDbConfig().setDbPath(dLedgerConfig.getDataStorePath());
        rdb = new RDB(configManager);

    }

    public void rocksdbInitial() {
        rdb.init();
        wb = new WriteBatch();
    }

    public void startup() {
        rocksdbInitial();
        load();
        recover();
        flushDataService.start();
        cleanSpaceService.start();
    }

    public void load() {
        if (!hasLoaded.compareAndSet(false, true)) {
            return;
        }
        if (!this.indexFileList.load()) {
            logger.error("Load file failed, this usually indicates fatal error, you should check it manually");
            System.exit(-1);
        }
    }

    public void shutdown() {
        flush();
        logger.info("maxWrotePos is [{}], flush is [{}]", this.indexFileList.getMaxWrotePosition(), this.indexFileList.getFlushedWhere());
        rdb.close();
//        this.indexFileList.shutdown(300);
        persistCheckPoint();
        cleanSpaceService.shutdown();
        flushDataService.shutdown();
    }

    public void recover() {
        if (!hasRecovered.compareAndSet(false, true)) {
            return;
        }
        PreConditions.check(indexFileList.checkSelf(), DLedgerResponseCode.DISK_ERROR, "check index file order failed before recovery");
        final List<MmapFile> mappedFiles = this.indexFileList.getMappedFiles();
        if (mappedFiles.isEmpty()) {
            return;
        }

        MmapFile lastMappedFile = indexFileList.getLastMappedFile();
        int index = mappedFiles.size() - 3;
        if (index < 0) {
            index = 0;
        }

        MmapFile mappedFile = mappedFiles.get(index);
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
        logger.info("Begin to recover data from  fileIndex={} fileSize={} fileName={} ", index, mappedFiles.size(), mappedFile.getFileName());
        long lastEntryIndex = -1;
        long lastTimestamp = 0;
        long lastEntryTerm = -1;
        long indexProcessOffset = mappedFile.getFileFromOffset();
        while (true) {
            try {
                int relativePos = byteBuffer.position();
                int magic = byteBuffer.getInt();
                long indexFromIndex = byteBuffer.getLong();
                long indexTimestamp = byteBuffer.getLong();
                long indexEntryTerm = byteBuffer.getLong();
                PreConditions.check(CURRENT_MAGIC == magic, DLedgerResponseCode.DISK_ERROR, "relativePos=%d, magic %d != %d", relativePos, magic, CURRENT_MAGIC);
                if (lastEntryIndex != -1) {
                    PreConditions.check(indexFromIndex == lastEntryIndex + 1, DLedgerResponseCode.DISK_ERROR, "relativePos=%d, indexFromIndex %d != %d", relativePos, indexFromIndex, lastEntryIndex);
                }
                PreConditions.check(indexTimestamp >= lastTimestamp, DLedgerResponseCode.DISK_ERROR, "relativePos=%d, indexTimestamp %d != %d", relativePos, indexFromIndex, lastTimestamp);
                PreConditions.check(indexEntryTerm >= lastEntryTerm, DLedgerResponseCode.DISK_ERROR, "relativePos=%d, indexEntryTerm %d != %d", relativePos, indexFromIndex, lastEntryTerm);
                PreConditions.check(indexProcessOffset == indexFromIndex * INDEX_UNIT_SIZE, DLedgerResponseCode.DISK_ERROR, "relativePos=%d, pos %d != %d", relativePos, indexProcessOffset, indexFromIndex * INDEX_UNIT_SIZE);
                lastEntryIndex = indexFromIndex;
                lastEntryTerm = indexEntryTerm;
                indexProcessOffset += INDEX_UNIT_SIZE;
                if (relativePos + INDEX_UNIT_SIZE == mappedFile.getFileSize()) {
                    index++;
                    if (index >= mappedFiles.size()) {
                        logger.info("Recover data file over, the last file is {}", mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        indexProcessOffset = mappedFile.getFileFromOffset();
                        logger.info("Trying to recover data file {}", mappedFile.getFileName());
                        continue;
                    }
                }
            } catch (Throwable t) {
                logger.warn("Pre check data and index failed {}", mappedFile.getFileName(), t);
                break;
            }
        }

        logger.info("Recover data to the end entryIndex={} indexProcessOffset={} lastFileOffset={} relativeOffset={}",
                lastEntryIndex, indexProcessOffset, lastMappedFile.getFileFromOffset(), indexProcessOffset - lastMappedFile.getFileFromOffset());
        if (lastMappedFile.getFileFromOffset() - indexProcessOffset > lastMappedFile.getFileSize()) {
            logger.error("[MONITOR]The processOffset is too small, you should check it manually before truncating the data from {}", indexProcessOffset);
            System.exit(-1);
        }

        ledgerEndIndex = lastEntryIndex;
        ledgerEndTerm = lastEntryTerm;
        if (lastEntryIndex != -1) {
            DLedgerEntry entry = get(lastEntryIndex);
            PreConditions.check(entry != null, DLedgerResponseCode.DISK_ERROR, "recheck get null entry");
            PreConditions.check(entry.getIndex() == lastEntryIndex, DLedgerResponseCode.DISK_ERROR, "recheck entry index %d != lastEntryIndex %d", entry.getIndex(), lastEntryIndex);
            reviseLedgerBeginIndex();
        }

        this.indexFileList.updateWherePosition(indexProcessOffset);
        this.indexFileList.truncateOffset(indexProcessOffset);
        updateLedgerEndIndexAndTerm();
        PreConditions.check(indexFileList.checkSelf(), DLedgerResponseCode.DISK_ERROR, "check index file order failed after recovery");
        //Load the committed index from checkpoint
        Properties properties = loadCheckPoint();
        if (properties == null || !properties.containsKey(COMMITTED_INDEX_KEY)) {
            return;
        }
        String committedIndexStr = String.valueOf(properties.get(COMMITTED_INDEX_KEY)).trim();
        if (committedIndexStr.length() <= 0) {
            return;
        }
        logger.info("Recover to get committed index={} from checkpoint", committedIndexStr);
        updateCommittedIndex(memberState.currTerm(), Long.valueOf(committedIndexStr));

    }

    public long getWritePos() {
        return indexFileList.getMaxWrotePosition();
    }

    public long getFlushPos() {
        return indexFileList.getFlushedWhere();
    }

    public void flush() {
        rdb.flush();
        this.indexFileList.flush(0);

    }

    private void reviseLedgerBeginIndex() {
        //get ledger begin index
        MmapFile firstFile = indexFileList.getFirstMappedFile();
        SelectMmapBufferResult sbr = firstFile.selectMappedBuffer(0);
        try {
            ByteBuffer tmpBuffer = sbr.getByteBuffer();
            tmpBuffer.position(firstFile.getStartPosition());
            tmpBuffer.getInt(); //magic
            ledgerBeginIndex = tmpBuffer.getLong();
        } finally {
            SelectMmapBufferResult.release(sbr);
        }

    }

    @Override
    public DLedgerEntry appendAsLeader(DLedgerEntry entry) {
        PreConditions.check(memberState.isLeader(), DLedgerResponseCode.NOT_LEADER);
        PreConditions.check(!isDiskFull, DLedgerResponseCode.DISK_FULL);
        ByteBuffer indexBuffer = localIndexBuffer.get();
        synchronized (memberState) {
            PreConditions.check(memberState.isLeader(), DLedgerResponseCode.NOT_LEADER);
            long nextIndex = ledgerEndIndex + 1;
            entry.setIndex(nextIndex);
            entry.setTerm(memberState.currTerm());
            entry.setMagic(CURRENT_MAGIC);
            writeData(entry);

            DLedgerRocksdbEntryCoder.encodeIndex(CURRENT_MAGIC, nextIndex, entry.getTimestamp(), memberState.currTerm(), indexBuffer);
            long indexPos = indexFileList.append(indexBuffer.array(), 0, indexBuffer.remaining(), false);
            PreConditions.check(indexPos == entry.getIndex() * INDEX_UNIT_SIZE, DLedgerResponseCode.DISK_ERROR, "appendAsLeader indexPos %d != entryPos %d", indexPos, entry.getIndex() * INDEX_UNIT_SIZE);
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] Append as Leader index:{} bodyLength:{}", memberState.getSelfId(), entry.getIndex(), entry.getBody().length);
            }
            ledgerEndIndex++;
            if (ledgerBeginIndex == -1) {
                ledgerBeginIndex = ledgerEndIndex;
            }
            updateLedgerEndIndexAndTerm();
            return entry;
        }
    }


    public void writeData(DLedgerEntry entry) {

        String key = String.valueOf(entry.getTimestamp()) + "_" + String.valueOf(entry.getIndex());

        if (logger.isDebugEnabled()) {
            logger.debug("write value to db key is {}, value is {}", key, new String(entry.getBody()));
        }

        wb.put(rdb.getDefaultCfHandle(), key.getBytes(), entry.getBody());
        rdb.writeAsync(wb);
        wb.clear();

    }

    private boolean isKeyHaveIndex(String key, Long index) {
        String[] splitArr = key.split(KEY_SEPARATOR);
        return splitArr[1].equals(index.toString());
    }

    private String makeKey(Long index, Long timestamp) {
        return timestamp.toString() + KEY_SEPARATOR + index.toString();
    }

    @Override
    public DLedgerEntry get(Long index) {
        PreConditions.check(index >= 0, DLedgerResponseCode.INDEX_OUT_OF_RANGE, "%d should gt 0", index);
        PreConditions.check(index <= ledgerEndIndex && index >= ledgerBeginIndex, DLedgerResponseCode.INDEX_OUT_OF_RANGE, "%d should between %d-%d", index, ledgerBeginIndex, ledgerEndIndex);
        Long timestamp = getTimestampFromFile(index);
        String key = makeKey(index, timestamp);
        if (logger.isDebugEnabled()) {
            logger.error("[{}] get key is {}, index is {}", memberState.getSelfId(), key, index);
        }
        DLedgerEntry dLedgerEntry = new DLedgerEntry();
        dLedgerEntry.setMagic(CURRENT_MAGIC);
        dLedgerEntry.setIndex(index);
        dLedgerEntry.setTimestamp(timestamp);
        dLedgerEntry.setBody(rdb.getDefault(key.getBytes()));
        if (logger.isDebugEnabled()) {
            logger.debug("[{}] get from leader index:{} body:{}", memberState.getSelfId(), dLedgerEntry.getIndex(), new String(dLedgerEntry.getBody()));
        }

        return dLedgerEntry;
    }

    private Long getTimestampFromFile(Long index) {
        SelectMmapBufferResult indexSbr = null;
        long indexOffset = index * INDEX_UNIT_SIZE;
        try {
            indexSbr = indexFileList.getData(indexOffset, INDEX_UNIT_SIZE);
            PreConditions.check(indexSbr != null && indexSbr.getByteBuffer() != null, DLedgerResponseCode.DISK_ERROR, "Get null index for %d", index);
            indexSbr.getByteBuffer().getInt(); //magic
            indexSbr.getByteBuffer().getLong(); //index
            return indexSbr.getByteBuffer().getLong(); //timestamp
        } finally {
            SelectMmapBufferResult.release(indexSbr);
        }
    }

    public List<DLedgerEntry> getList(Long timestamp) {
        RocksIterator it = rdb.newIteratorDefault();
        List<DLedgerEntry> dLedgerEntries = new ArrayList<>(2000);
        byte[] now = timestamp.toString().getBytes();
        for (it.seek(now); it.isValid(); it.next()) {
            DLedgerEntry dLedgerEntry = new DLedgerEntry();
            String key = new String(it.key());
            String[] splitArr = key.split(KEY_SEPARATOR);
            dLedgerEntry.setTimestamp(timestamp);
            dLedgerEntry.setIndex(Long.parseLong(splitArr[1]));
            dLedgerEntry.setBody(it.value());
            dLedgerEntries.add(dLedgerEntry);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("[{}] getList size is  [{}] first element is [{}]", memberState.getSelfId(), dLedgerEntries.size(), dLedgerEntries.get(0));
        }
        return dLedgerEntries;
    }


    private void deleteDataInRocksdb(long index, Long timestamp) {
        String key = makeKey(index, timestamp);
        rdb.deleteDefault(key.getBytes());
    }

    @Override
    public long getLedgerBeginIndex() {
        return ledgerBeginIndex;
    }

    private void deleteData(Long truncateIndexOffset) {
        SelectMmapBufferResult indexSbr = null;
        long endIndexOffset = getLedgerEndIndex() * INDEX_UNIT_SIZE;

        while (truncateIndexOffset <= endIndexOffset) {
            try {
                indexSbr = indexFileList.getData(truncateIndexOffset, INDEX_UNIT_SIZE);
                PreConditions.check(indexSbr != null && indexSbr.getByteBuffer() != null, DLedgerResponseCode.DISK_ERROR, "Get null index for %d", truncateIndexOffset);
                indexSbr.getByteBuffer().getInt(); //magic
                long index = indexSbr.getByteBuffer().getLong();
                long timestamp = indexSbr.getByteBuffer().getLong();
                deleteDataInRocksdb(index, timestamp);
            } finally {
                SelectMmapBufferResult.release(indexSbr);
            }
            truncateIndexOffset += INDEX_UNIT_SIZE;
        }


    }

    @Override
    public long truncate(DLedgerEntry entry, long leaderTerm, String leaderId) {
        PreConditions.check(memberState.isFollower(), DLedgerResponseCode.NOT_FOLLOWER, null);
        ByteBuffer indexBuffer = localIndexBuffer.get();
        synchronized (memberState) {
            PreConditions.check(memberState.isFollower(), DLedgerResponseCode.NOT_FOLLOWER, "role=%s", memberState.getRole());
            PreConditions.check(leaderTerm == memberState.currTerm(), DLedgerResponseCode.INCONSISTENT_TERM, "term %d != %d", leaderTerm, memberState.currTerm());
            PreConditions.check(leaderId.equals(memberState.getLeaderId()), DLedgerResponseCode.INCONSISTENT_LEADER, "leaderId %s != %s", leaderId, memberState.getLeaderId());

            long truncateIndexOffset = entry.getIndex() * INDEX_UNIT_SIZE;
            deleteData(truncateIndexOffset);
            writeData(entry);

            indexFileList.truncateOffset(truncateIndexOffset);
            if (indexFileList.getMaxWrotePosition() != truncateIndexOffset) {
                logger.warn("[TRUNCATE] rebuild for index wrotePos: {} != truncatePos: {}", indexFileList.getMaxWrotePosition(), truncateIndexOffset);
                //rebuildWithPos means read and write position start with this pos
                PreConditions.check(indexFileList.rebuildWithPos(truncateIndexOffset), DLedgerResponseCode.DISK_ERROR, "rebuild index truncatePos=%d", truncateIndexOffset);
            }
            DLedgerRocksdbEntryCoder.encodeIndex(entry.getMagic(), entry.getIndex(), entry.getTimestamp(), entry.getTerm(), indexBuffer);
            long indexPos = indexFileList.append(indexBuffer.array(), 0, indexBuffer.remaining(), false);
            PreConditions.check(indexPos == entry.getIndex() * INDEX_UNIT_SIZE, DLedgerResponseCode.DISK_ERROR, null);
            ledgerEndTerm = memberState.currTerm();
            ledgerEndIndex = entry.getIndex();
            reviseLedgerBeginIndex();
            updateLedgerEndIndexAndTerm();
            return entry.getIndex();
        }
    }

    @Override
    public DLedgerEntry appendAsFollower(DLedgerEntry entry, long leaderTerm, String leaderId) {
        PreConditions.check(memberState.isFollower(), DLedgerResponseCode.NOT_FOLLOWER);
        ByteBuffer indexBuffer = localIndexBuffer.get();
        synchronized (memberState) {
            PreConditions.check(memberState.isFollower(), DLedgerResponseCode.NOT_FOLLOWER);
            long nextIndex = ledgerEndIndex + 1;
            PreConditions.check(nextIndex == entry.getIndex(), DLedgerResponseCode.INCONSISTENT_INDEX, null);
            PreConditions.check(leaderTerm == memberState.currTerm(), DLedgerResponseCode.INCONSISTENT_TERM, null);
            PreConditions.check(leaderId.equals(memberState.getLeaderId()), DLedgerResponseCode.INCONSISTENT_LEADER, null);
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] Append as Follower {} {}", memberState.getSelfId(), entry.getIndex(), entry.getBody().length);
            }
            writeData(entry);
            DLedgerRocksdbEntryCoder.encodeIndex(entry.getMagic(), entry.getIndex(), entry.getTimestamp(), entry.getTerm(), indexBuffer);
            long indexPos = indexFileList.append(indexBuffer.array(), 0, indexBuffer.remaining(), false);
            PreConditions.check(indexPos == entry.getIndex() * INDEX_UNIT_SIZE, DLedgerResponseCode.DISK_ERROR, null);
            ledgerEndTerm = memberState.currTerm();
            ledgerEndIndex = entry.getIndex();
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

    public MmapFileList getIndexFileList() {
        return indexFileList;
    }

    void persistCheckPoint() {
        try {
            Properties properties = new Properties();
            properties.put(END_INDEX_KEY, getLedgerEndIndex());
            properties.put(COMMITTED_INDEX_KEY, getCommittedIndex());
            String data = IOUtils.properties2String(properties);
            IOUtils.string2File(data, dLedgerConfig.getDefaultPath() + File.separator + CHECK_POINT_FILE);
        } catch (Throwable t) {
            logger.error("Persist checkpoint failed", t);
        }
    }

    Properties loadCheckPoint() {
        try {
            String data = IOUtils.file2String(dLedgerConfig.getDefaultPath() + File.separator + CHECK_POINT_FILE);
            Properties properties = IOUtils.string2Properties(data);
            return properties;
        } catch (Throwable t) {
            logger.error("Load checkpoint failed", t);

        }
        return null;
    }

    public long getLedgerEndTerm() {
        return ledgerEndTerm;
    }

    public long getLedgerEndIndex() {
        return ledgerEndIndex;
    }

    @Override
    public MemberState getMemberState() {
        return memberState;
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

    public void deleteLastMappedFile(int size) {
        for (int i = 0; i < size; i++) {
            this.indexFileList.deleteLastMappedFile();
        }
    }

    class FlushDataService extends ShutdownAbleThread {

        public FlushDataService(String name, Logger logger) {
            super(name, logger);
        }

        @Override
        public void doWork() {
            try {
                long start = System.currentTimeMillis();
                DLedgerRocksdbStore.this.indexFileList.flush(0);
                if (DLedgerUtils.elapsed(start) > 500) {
                    logger.info("Flush data cost={} ms", DLedgerUtils.elapsed(start));
                }

                if (DLedgerUtils.elapsed(lastCheckPointTimeMs) > dLedgerConfig.getCheckPointInterval()) {
                    persistCheckPoint();
                    lastCheckPointTimeMs = System.currentTimeMillis();
                }

                waitForRunning(dLedgerConfig.getFlushFileInterval());
            } catch (Throwable t) {
                logger.info("Error in {}", getName(), t);
                DLedgerUtils.sleep(200);
            }
        }
    }

    class CleanSpaceService extends ShutdownAbleThread {

        double storeBaseRatio = DLedgerUtils.getDiskPartitionSpaceUsedPercent(dLedgerConfig.getStoreBaseDir());
        double dataRatio = DLedgerUtils.getDiskPartitionSpaceUsedPercent(dLedgerConfig.getDataStorePath());

        public CleanSpaceService(String name, Logger logger) {
            super(name, logger);
        }

        @Override
        public void doWork() {
            try {
                storeBaseRatio = DLedgerUtils.getDiskPartitionSpaceUsedPercent(dLedgerConfig.getStoreBaseDir());
                dataRatio = DLedgerUtils.getDiskPartitionSpaceUsedPercent(dLedgerConfig.getDataStorePath());
                long hourOfMs = 3600L * 1000L;
                long fileReservedTimeMs = dLedgerConfig.getFileReservedHours() * hourOfMs;
                if (fileReservedTimeMs < hourOfMs) {
                    logger.warn("The fileReservedTimeMs={} is smaller than hourOfMs={}", fileReservedTimeMs, hourOfMs);
                    fileReservedTimeMs = hourOfMs;
                }
                //If the disk is full, should prevent more data to get in
                DLedgerRocksdbStore.this.isDiskFull = isNeedForbiddenWrite();
                boolean timeUp = isTimeToDelete();
                boolean checkExpired = isNeedCheckExpired();
                boolean forceClean = isNeedForceClean();
                boolean enableForceClean = dLedgerConfig.isEnableDiskForceClean();
                if (timeUp || checkExpired) {
                    int count = getIndexFileList().deleteExpiredFileByTime(fileReservedTimeMs, 100, 120 * 1000, forceClean && enableForceClean);
                    if (count > 0 || (forceClean && enableForceClean) || isDiskFull) {
                        logger.info("Clean space count={} timeUp={} checkExpired={} forceClean={} enableForceClean={} diskFull={} storeBaseRatio={} dataRatio={}",
                                count, timeUp, checkExpired, forceClean, enableForceClean, isDiskFull, storeBaseRatio, dataRatio);
                    }
                    if (count > 0) {
                        DLedgerRocksdbStore.this.reviseLedgerBeginIndex();
                    }
                }
                waitForRunning(100);
            } catch (Throwable t) {
                logger.info("Error in {}", getName(), t);
                DLedgerUtils.sleep(200);
            }
        }

        private boolean isTimeToDelete() {
            String when = DLedgerRocksdbStore.this.dLedgerConfig.getDeleteWhen();
            if (DLedgerUtils.isItTimeToDo(when)) {
                return true;
            }

            return false;
        }

        private boolean isNeedCheckExpired() {
            if (storeBaseRatio > dLedgerConfig.getDiskSpaceRatioToCheckExpired()
                    || dataRatio > dLedgerConfig.getDiskSpaceRatioToCheckExpired()) {
                return true;
            }
            return false;
        }

        private boolean isNeedForceClean() {
            if (storeBaseRatio > dLedgerConfig.getDiskSpaceRatioToForceClean()
                    || dataRatio > dLedgerConfig.getDiskSpaceRatioToForceClean()) {
                return true;
            }
            return false;
        }

        private boolean isNeedForbiddenWrite() {
            if (storeBaseRatio > dLedgerConfig.getDiskFullRatio()
                    || dataRatio > dLedgerConfig.getDiskFullRatio()) {
                return true;
            }
            return false;
        }
    }
}
