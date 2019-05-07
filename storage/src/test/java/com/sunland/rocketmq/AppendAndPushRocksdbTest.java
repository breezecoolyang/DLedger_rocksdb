package com.sunland.rocketmq;

import com.sunland.rocketmq.protocol.AppendEntryRequest;
import com.sunland.rocketmq.protocol.AppendEntryResponse;
import com.sunland.rocketmq.protocol.DLedgerResponseCode;
import com.sunland.rocketmq.entry.DLedgerEntry;
import com.sunland.rocketmq.utils.DLedgerUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;


public class AppendAndPushRocksdbTest extends ServerTestHarness{
    @Test
    public void testSingleServerInRocksdbLoadAndRecover() throws Exception {
        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String peers = String.format("n0-localhost:%d", nextPort());
        DLedgerServer dLedgerServer0 = launchServer(group, peers, selfId, selfId, DLedgerConfig.ROCKSDB);
        for (int i = 0; i < 10; i++) {
            DLedgerEntry entry = new DLedgerEntry();
            entry.setBody((" hello testSingleServerInRocksdbLoad" + i).getBytes());
            entry.setTimestamp(1536811275);
            DLedgerEntry resEntry = dLedgerServer0.getdLedgerStore().appendAsLeader(entry);
            Assert.assertEquals(i, resEntry.getIndex());
        }

        dLedgerServer0.shutdown();

        dLedgerServer0 = launchServer(group, peers, "n0", "n0", DLedgerConfig.ROCKSDB);

        for (long i = 0; i < 10; i++) {
            DLedgerEntry entry = dLedgerServer0.getdLedgerStore().get(i);
            Assert.assertEquals(i, entry.getIndex());
            Assert.assertEquals(1536811275, entry.getTimestamp());
            Assert.assertArrayEquals((" hello testSingleServerInRocksdbLoad" + i).getBytes(), entry.getBody());
        }

    }

    @Test
    public void testPushCommittedIndex() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort());

        DLedgerServer dLedgerServer0 = launchServer(group, peers, "n0", "n0", DLedgerConfig.ROCKSDB);
        List<CompletableFuture<AppendEntryResponse>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setGroup(group);
            appendEntryRequest.setRemoteId(dLedgerServer0.getMemberState().getSelfId());
            appendEntryRequest.setBody(new byte[128]);
            appendEntryRequest.setTimestamp(1536811267);
            CompletableFuture<AppendEntryResponse> future = dLedgerServer0.handleAppend(appendEntryRequest);
            Assert.assertTrue(future instanceof AppendFuture);
            futures.add(future);
        }
        Assert.assertEquals(9, dLedgerServer0.getdLedgerStore().getLedgerEndIndex());
        Assert.assertEquals(-1, dLedgerServer0.getdLedgerStore().getCommittedIndex());
        DLedgerServer dLedgerServer1 = launchServer(group, peers, "n1", "n0", DLedgerConfig.ROCKSDB);
        long start = System.currentTimeMillis();
        while (DLedgerUtils.elapsed(start) < 3000 && dLedgerServer1.getdLedgerStore().getCommittedIndex() != 9) {
            DLedgerUtils.sleep(100);
        }
        Assert.assertEquals(9, dLedgerServer0.getdLedgerStore().getCommittedIndex());
        Assert.assertEquals(9, dLedgerServer1.getdLedgerStore().getCommittedIndex());
    }

    @Test
    public void testPushNetworkOffline() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort());

        DLedgerServer dLedgerServer0 = launchServer(group, peers, "n0", "n0", DLedgerConfig.ROCKSDB);
        List<CompletableFuture<AppendEntryResponse>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setGroup(group);
            appendEntryRequest.setRemoteId(dLedgerServer0.getMemberState().getSelfId());
            appendEntryRequest.setBody(new byte[128]);
            appendEntryRequest.setTimestamp(1536811268);
            CompletableFuture<AppendEntryResponse> future = dLedgerServer0.handleAppend(appendEntryRequest);
            Assert.assertTrue(future instanceof AppendFuture);
            futures.add(future);
        }
        Assert.assertEquals(9, dLedgerServer0.getdLedgerStore().getLedgerEndIndex());
        Thread.sleep(dLedgerServer0.getdLedgerConfig().getMaxWaitAckTimeMs() + 100);
        for (int i = 0; i < futures.size(); i++) {
            CompletableFuture<AppendEntryResponse> future = futures.get(i);
            Assert.assertTrue(future.isDone());
            Assert.assertEquals(DLedgerResponseCode.WAIT_QUORUM_ACK_TIMEOUT.getCode(), future.get().getCode());
        }

        boolean hasWait = false;
        for (int i = 0; i < dLedgerServer0.getdLedgerConfig().getMaxPendingRequestsNum(); i++) {
            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setGroup(group);
            appendEntryRequest.setRemoteId(dLedgerServer0.getMemberState().getSelfId());
            appendEntryRequest.setBody(new byte[128]);
            appendEntryRequest.setTimestamp(1536811269);
            CompletableFuture<AppendEntryResponse> future = dLedgerServer0.handleAppend(appendEntryRequest);
            Assert.assertTrue(future instanceof AppendFuture);
            if (future.isDone()) {
                Assert.assertEquals(DLedgerResponseCode.LEADER_PENDING_FULL.getCode(), future.get().getCode());
                hasWait = true;
                break;
            }
        }
        Assert.assertTrue(hasWait);
    }

    @Test
    public void testPushNetworkNotStable() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort());

        DLedgerServer dLedgerServer0 = launchServer(group, peers, "n0", "n0", DLedgerConfig.ROCKSDB);
        AtomicBoolean sendSuccess = new AtomicBoolean(false);
        AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
        appendEntryRequest.setGroup(group);
        appendEntryRequest.setRemoteId(dLedgerServer0.getMemberState().getSelfId());
        appendEntryRequest.setBody("helllo".getBytes());
        appendEntryRequest.setTimestamp(1536811269);
        CompletableFuture<AppendEntryResponse> future = dLedgerServer0.handleAppend(appendEntryRequest);
        Assert.assertTrue(future instanceof AppendFuture);
        future.whenComplete((x, ex) -> {
            sendSuccess.set(true);
        });
        Thread.sleep(500);
        Assert.assertTrue(!sendSuccess.get());
        //start server1
        DLedgerServer dLedgerServer1 = launchServer(group, peers, "n1", "n0", DLedgerConfig.ROCKSDB);
        Thread.sleep(1500);
        Assert.assertTrue(sendSuccess.get());
        //shutdown server1
        dLedgerServer1.shutdown();
        sendSuccess.set(false);
        future = dLedgerServer0.handleAppend(appendEntryRequest);
        Assert.assertTrue(future instanceof AppendFuture);
        future.whenComplete((x, ex) -> {
            sendSuccess.set(true);
        });
        Thread.sleep(500);
        Assert.assertTrue(!sendSuccess.get());
        //restart servre1
        dLedgerServer1 = launchServer(group, peers, "n1", "n0", DLedgerConfig.ROCKSDB);
        Thread.sleep(1500);
        Assert.assertTrue(sendSuccess.get());

        Assert.assertEquals(0, dLedgerServer0.getdLedgerStore().getLedgerBeginIndex());
        Assert.assertEquals(1, dLedgerServer0.getdLedgerStore().getLedgerEndIndex());
        Assert.assertEquals(0, dLedgerServer1.getdLedgerStore().getLedgerBeginIndex());
        Assert.assertEquals(1, dLedgerServer1.getdLedgerStore().getLedgerEndIndex());
    }

    @Test
    public void testPushMissed() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort());
        DLedgerServer dLedgerServer0 = launchServer(group, peers, "n0", "n0", DLedgerConfig.ROCKSDB);
        DLedgerServer dLedgerServer1 = launchServer(group, peers, "n1", "n0", DLedgerConfig.ROCKSDB);
        DLedgerServer mockServer1 = Mockito.spy(dLedgerServer1);
        AtomicInteger callNum = new AtomicInteger(0);
        doAnswer(x -> {
            if (callNum.incrementAndGet() % 3 == 0) {
                return new CompletableFuture<>();
            } else {
                return dLedgerServer1.handlePush(x.getArgument(0));
            }
        }).when(mockServer1).handlePush(any());
        ((DLedgerRpcNettyService) dLedgerServer1.getdLedgerRpcService()).setdLedgerServer(mockServer1);

        for (int i = 0; i < 10; i++) {
            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setGroup(group);
            appendEntryRequest.setTimestamp(1536811270);
            appendEntryRequest.setBody(new byte[128]);
            appendEntryRequest.setRemoteId(dLedgerServer0.getMemberState().getSelfId());
            AppendEntryResponse appendEntryResponse = dLedgerServer0.handleAppend(appendEntryRequest).get(3, TimeUnit.SECONDS);
            Assert.assertEquals(appendEntryResponse.getCode(), DLedgerResponseCode.SUCCESS.getCode());
            Assert.assertEquals(i, appendEntryResponse.getIndex());
        }
        Assert.assertEquals(0, dLedgerServer0.getdLedgerStore().getLedgerBeginIndex());
        Assert.assertEquals(9, dLedgerServer0.getdLedgerStore().getLedgerEndIndex());

        Assert.assertEquals(0, dLedgerServer1.getdLedgerStore().getLedgerBeginIndex());
        Assert.assertEquals(9, dLedgerServer1.getdLedgerStore().getLedgerEndIndex());
    }

    @Test
    public void testPushTruncate() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort());
        DLedgerServer dLedgerServer0 = launchServer(group, peers, "n0", "n0", DLedgerConfig.ROCKSDB);
        for (int i = 0; i < 10; i++) {
            DLedgerEntry entry = new DLedgerEntry();
            entry.setBody(new byte[128]);
            entry.setTimestamp(1536811271);
            DLedgerEntry resEntry = dLedgerServer0.getdLedgerStore().appendAsLeader(entry);
            Assert.assertEquals(i, resEntry.getIndex());
        }
        Assert.assertEquals(0, dLedgerServer0.getdLedgerStore().getLedgerBeginIndex());
        Assert.assertEquals(9, dLedgerServer0.getdLedgerStore().getLedgerEndIndex());
        List<DLedgerEntry> entries = new ArrayList<>();
        for (long i = 0; i < 10; i++) {
            entries.add(dLedgerServer0.getdLedgerStore().get(i));
        }
        dLedgerServer0.shutdown();

        DLedgerServer dLedgerServer1 = launchServer(group, peers, "n1", "n0", DLedgerConfig.ROCKSDB);
        for (int i = 0; i < 5; i++) {
            DLedgerEntry resEntry = dLedgerServer1.getdLedgerStore().appendAsFollower(entries.get(i), 0, "n0");
            Assert.assertEquals(i, resEntry.getIndex());
        }
        dLedgerServer1.shutdown();

        //change leader from n0 => n1
        dLedgerServer1 = launchServer(group, peers, "n1", "n1", DLedgerConfig.ROCKSDB);
        dLedgerServer0 = launchServer(group, peers, "n0", "n1", DLedgerConfig.ROCKSDB);
        Thread.sleep(1000);
        Assert.assertEquals(0, dLedgerServer0.getdLedgerStore().getLedgerBeginIndex());
        Assert.assertEquals(4, dLedgerServer0.getdLedgerStore().getLedgerEndIndex());
        Assert.assertEquals(0, dLedgerServer1.getdLedgerStore().getLedgerBeginIndex());
        Assert.assertEquals(4, dLedgerServer1.getdLedgerStore().getLedgerEndIndex());
        for (int i = 0; i < 10; i++) {
            AppendEntryRequest request = new AppendEntryRequest();
            request.setGroup(group);
            request.setRemoteId(dLedgerServer1.getMemberState().getSelfId());
            request.setBody(new byte[128]);
            request.setTimestamp(1536811271);
            long appendIndex = dLedgerServer1.handleAppend(request).get().getIndex();
            Assert.assertEquals(i + 5, appendIndex);
        }
    }
}
