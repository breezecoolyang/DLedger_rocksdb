package com.sunland.rocketmq.rocksdbStore;

import com.sunland.rocketmq.ServerTestHarness;
import com.sunland.rocketmq.store.rocksdb.DLedgerRocksdbStore;
import com.sunland.rocketmq.util.FileTestUtil;
import com.sunland.rocketmq.DLedgerConfig;
import com.sunland.rocketmq.MemberState;
import com.sunland.rocketmq.entry.DLedgerEntry;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

public class DLedgerRocksdbStoreTest extends ServerTestHarness {

    public synchronized DLedgerRocksdbStore createRocksdbStore(String group, String peers, String selfId, String leaderId, int indexFileSize) {
        DLedgerConfig config = new DLedgerConfig();
        config.group(group).selfId(selfId).peers(peers);
        String dataDir = "dledger-" + config.getSelfId();
        System.setProperty("log.middle.dir", dataDir);
        config.setStoreBaseDir(FileTestUtil.TEST_BASE + File.separator + group);
        config.setStoreType(DLedgerConfig.ROCKSDB);
        config.setMappedFileSizeForRocksDBEntryIndex(10 * 1024 * 1024);
        config.setEnableLeaderElector(false);
        config.setEnableDiskForceClean(false);
        config.setDiskSpaceRatioToForceClean(0.90f);

        if (indexFileSize != -1) {
            config.setMappedFileSizeForRocksDBEntryIndex(indexFileSize);
        }

        MemberState memberState = new MemberState(config);
        memberState.setCurrTermForTest(0);
        if (selfId.equals(leaderId)) {
            memberState.changeToLeader(0);
        } else {
            memberState.changeToFollower(0, leaderId);
        }
        bases.add(config.getDataStorePath());
        bases.add(config.getIndexStorePath());
        bases.add(config.getDefaultPath());

        DLedgerRocksdbStore fileStore = new DLedgerRocksdbStore(config, memberState);
        fileStore.startup();
        return fileStore;
    }

    public synchronized DLedgerRocksdbStore createRocksdbStore(String group, String peers, String selfId, String leaderId) {
        return createRocksdbStore(group, peers, selfId, leaderId, DLedgerRocksdbStore.INDEX_UNIT_SIZE * 1024 * 1024);
    }

    @Test
    public void testCommittedIndex() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d", nextPort());

        DLedgerRocksdbStore fileStore =  createRocksdbStore(group,  peers, "n0", "n0");
        MemberState memberState = fileStore.getMemberState();
        for (int i = 0; i < 100; i++) {
            DLedgerEntry entry = new DLedgerEntry();
            entry.setBody((new byte[128]));
            entry.setTimestamp(1536811267);
            DLedgerEntry resEntry = fileStore.appendAsLeader(entry);
            Assert.assertEquals(i, resEntry.getIndex());
        }
        fileStore.updateCommittedIndex(memberState.currTerm(), 90);
        Assert.assertEquals(99, fileStore.getLedgerEndIndex());
        Assert.assertEquals(90, fileStore.getCommittedIndex());

        while (fileStore.getFlushPos() != fileStore.getWritePos()) {
            fileStore.flush();
        }
        fileStore.shutdown();
        fileStore = createRocksdbStore(group, peers, "n0", "n0");
        Assert.assertEquals(0, fileStore.getLedgerBeginIndex());
        Assert.assertEquals(99, fileStore.getLedgerEndIndex());
        Assert.assertEquals(90, fileStore.getCommittedIndex());
    }

    @Test
    public void testAppendAsLeader() throws Exception {
        String group = UUID.randomUUID().toString();
        DLedgerRocksdbStore fileStore = createRocksdbStore(group, "n0-localhost:20911", "n0", "n0");
        for (int i = 0; i < 10; i++) {
            DLedgerEntry entry = new DLedgerEntry();
            entry.setBody(("Hello Leader" + i).getBytes());
            entry.setTimestamp(1536811267);
            DLedgerEntry resEntry = fileStore.appendAsLeader(entry);
            Assert.assertEquals(i, resEntry.getIndex());
        }
        for (long i = 0; i < 10; i++) {
            DLedgerEntry entry = fileStore.get(i);
            Assert.assertEquals(i, entry.getIndex());
            Assert.assertArrayEquals(("Hello Leader" + i).getBytes(), entry.getBody());
        }

        for (long i = 0; i < 10; i++) {
            fileStore.updateCommittedIndex(0, i);
            Assert.assertEquals(i, fileStore.getCommittedIndex());
        }

        //ignore the smaller index and smaller term
        fileStore.updateCommittedIndex(0, -1);
        Assert.assertEquals(9, fileStore.getLedgerEndIndex());
        fileStore.updateCommittedIndex(0, 0);
        Assert.assertEquals(9, fileStore.getLedgerEndIndex());
        fileStore.updateCommittedIndex(-1, 10);
        Assert.assertEquals(9, fileStore.getLedgerEndIndex());
    }

    @Test
    public void testNormalRecovery() {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d", nextPort());
        DLedgerRocksdbStore fileStore = createRocksdbStore(group, peers, "n0", "n0");
        for (int i = 0; i < 10; i++) {
            DLedgerEntry entry = new DLedgerEntry();
            entry.setBody(("Hello Leader With Rocksdb Recovery" + i).getBytes());
            DLedgerEntry resEntry = fileStore.appendAsLeader(entry);
            Assert.assertEquals(i, resEntry.getIndex());
        }
        while (fileStore.getFlushPos() != fileStore.getWritePos()) {
            fileStore.flush();
        }
        fileStore.shutdown();
        fileStore = createRocksdbStore(group, peers, "n0", "n0");
        Assert.assertEquals(0, fileStore.getLedgerBeginIndex());
        Assert.assertEquals(9, fileStore.getLedgerEndIndex());
        for (long i = 0; i < 10; i++) {
            DLedgerEntry entry = fileStore.get(i);
            Assert.assertEquals(i, entry.getIndex());
            Assert.assertArrayEquals(("Hello Leader With Rocksdb Recovery" + i).getBytes(), entry.getBody());
        }
    }

    @Test
    public void testTruncate() {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d", nextPort());
        DLedgerRocksdbStore fileStore = createRocksdbStore(group, peers, "n0", "n0", 8 * DLedgerRocksdbStore.INDEX_UNIT_SIZE);
        DLedgerRocksdbStore otherFileStore = createRocksdbStore(group, peers, "n1", "n1", 8 * DLedgerRocksdbStore.INDEX_UNIT_SIZE);

        for (int i = 0; i < 10; i++) {
            DLedgerEntry entry = new DLedgerEntry();
            entry.setBody(new byte[128]);
            DLedgerEntry resEntry = fileStore.appendAsLeader(entry);
            Assert.assertEquals(i, resEntry.getIndex());
            DLedgerEntry reEntry = otherFileStore.appendAsLeader(entry);
            Assert.assertEquals(i, reEntry.getIndex());
        }
        Assert.assertEquals(2, fileStore.getIndexFileList().getMappedFiles().size());
        Assert.assertEquals(0, fileStore.getLedgerBeginIndex());
        Assert.assertEquals(9, fileStore.getLedgerEndIndex());
        fileStore.getMemberState().changeToFollower(fileStore.getLedgerEndTerm(), "n1");


        {
            //truncate the mid
            DLedgerEntry midEntry = otherFileStore.get(5L);
            Assert.assertNotNull(midEntry);
            long midIndex = fileStore.truncate(midEntry, fileStore.getLedgerEndTerm(), "n1");
            Assert.assertEquals(5, midIndex);
            Assert.assertEquals(0, fileStore.getLedgerBeginIndex());
            Assert.assertEquals(5, fileStore.getLedgerEndIndex());
            Assert.assertEquals((midIndex + 1) * DLedgerRocksdbStore.INDEX_UNIT_SIZE, fileStore.getIndexFileList().getMaxWrotePosition());
        }
        {
            //truncate just after
            DLedgerEntry afterEntry = otherFileStore.get(6L);
            Assert.assertNotNull(afterEntry);
            long afterIndex = fileStore.truncate(afterEntry, fileStore.getLedgerEndTerm(), "n1");
            Assert.assertEquals(6, afterIndex);
            Assert.assertEquals(0, fileStore.getLedgerBeginIndex());
            Assert.assertEquals(6, fileStore.getLedgerEndIndex());
            Assert.assertEquals((afterIndex + 1) * DLedgerRocksdbStore.INDEX_UNIT_SIZE, fileStore.getIndexFileList().getMaxWrotePosition());
        }

        {
            //truncate to the end
            DLedgerEntry endEntry = otherFileStore.get(9L);
            Assert.assertNotNull(endEntry);
            long endIndex = fileStore.truncate(endEntry, fileStore.getLedgerEndTerm(), "n1");
            Assert.assertEquals(9, endIndex);
            Assert.assertEquals(9, fileStore.getLedgerEndIndex());
            Assert.assertEquals(9, fileStore.getLedgerBeginIndex());
            Assert.assertEquals((endIndex + 1) * DLedgerRocksdbStore.INDEX_UNIT_SIZE, fileStore.getIndexFileList().getMaxWrotePosition());
        }

    }

    @Test
    public void testAppendAsFollower() {
        DLedgerRocksdbStore fileStore = createRocksdbStore(UUID.randomUUID().toString(), "n0-localhost:20913", "n0", "n1");
        for (int i = 0; i < 10; i++) {
            DLedgerEntry entry = new DLedgerEntry();
            entry.setTerm(0);
            entry.setIndex(i);
            entry.setBody(("Hello Follower" + i).getBytes());
            entry.setTimestamp(1536811267);
            DLedgerEntry resEntry = fileStore.appendAsFollower(entry, 0, "n1");
            Assert.assertEquals(i, resEntry.getIndex());
        }
        for (long i = 0; i < 10; i++) {
            DLedgerEntry entry = fileStore.get(i);
            Assert.assertEquals(i, entry.getIndex());
            Assert.assertArrayEquals(("Hello Follower" + i).getBytes(), entry.getBody());
        }
    }
}
