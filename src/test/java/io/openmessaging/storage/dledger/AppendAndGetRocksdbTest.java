package io.openmessaging.storage.dledger;

import io.openmessaging.storage.dledger.client.DLedgerClient;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.GetEntriesResponse;
import io.openmessaging.storage.dledger.protocol.GetListEntriesResponse;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class AppendAndGetRocksdbTest extends ServerTestHarness{

    @Test
    public void testSingleServerInRocksdb() throws Exception {
        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String peers = "n0-localhost:10005";
        launchServer(group, peers, selfId, selfId, DLedgerConfig.ROCKSDB);
        DLedgerClient dLedgerClient = launchClient(group, peers);
        for (long i = 0; i < 10; i++) {
            AppendEntryResponse appendEntryResponse = dLedgerClient.append(1536811267, ("HelloSingleServerInRocksdb" + i).getBytes());
            Assert.assertEquals(i, appendEntryResponse.getIndex());
        }

        GetListEntriesResponse getEntriesResponse = dLedgerClient.getByTime(1536811267);
        Assert.assertEquals(10, getEntriesResponse.getEntries().size());
        for (long i = 0; i < 10; i++) {
            Assert.assertEquals(i, getEntriesResponse.getEntries().get((int)i).getIndex());
            Assert.assertArrayEquals(("HelloSingleServerInRocksdb" + i).getBytes(), getEntriesResponse.getEntries().get((int)i).getBody());
        }

    }

    @Test
    public void testSingleServerInRocksdbGet() throws Exception {
        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String peers = String.format("n0-localhost:%d", nextPort());
        launchServer(group, peers, selfId, selfId, DLedgerConfig.ROCKSDB);
        DLedgerClient dLedgerClient = launchClient(group, peers);
        for (long i = 0; i < 10; i++) {
            AppendEntryResponse appendEntryResponse = dLedgerClient.append(1536811267, ("HelloSingleServerInRocksdbGet" + i).getBytes());
            Assert.assertEquals(i, appendEntryResponse.getIndex());
        }

        for (long i = 0; i < 10; i++) {
            GetEntriesResponse getEntriesResponse = dLedgerClient.get(i);
            Assert.assertEquals(1, getEntriesResponse.getEntries().size());
            Assert.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
            Assert.assertArrayEquals(("HelloSingleServerInRocksdbGet" + i).getBytes(), getEntriesResponse.getEntries().get(0).getBody());
        }

    }

    @Test
    public void testThreeServerInRocksdb() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = "n0-localhost:10006;n1-localhost:10007;n2-localhost:10008";
        DLedgerServer dLedgerServer0 = launchServer(group, peers, "n0", "n1", DLedgerConfig.ROCKSDB);
        DLedgerServer dLedgerServer1 = launchServer(group, peers, "n1", "n1", DLedgerConfig.ROCKSDB);
        DLedgerServer dLedgerServer2 = launchServer(group, peers, "n2", "n1", DLedgerConfig.ROCKSDB);
        DLedgerClient dLedgerClient = launchClient(group, peers);
        long timestamp = System.currentTimeMillis()/1000;
        for (int i = 0; i < 10; i++) {
            AppendEntryResponse appendEntryResponse = dLedgerClient.append(timestamp, ("HelloSingleServerInRocksdb" + i).getBytes());
            Assert.assertEquals(appendEntryResponse.getCode(), DLedgerResponseCode.SUCCESS.getCode());
            Assert.assertEquals(i, appendEntryResponse.getIndex());
        }
        Thread.sleep(100);
        Assert.assertEquals(9, dLedgerServer0.getdLedgerStore().getLedgerEndIndex());
        Assert.assertEquals(9, dLedgerServer1.getdLedgerStore().getLedgerEndIndex());
        Assert.assertEquals(9, dLedgerServer2.getdLedgerStore().getLedgerEndIndex());

        GetListEntriesResponse getEntriesResponse = dLedgerClient.getByTime(timestamp);
        Assert.assertEquals(10, getEntriesResponse.getEntries().size());
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(i, getEntriesResponse.getEntries().get(i).getIndex());
            Assert.assertArrayEquals(("HelloSingleServerInRocksdb" + i).getBytes(), getEntriesResponse.getEntries().get(i).getBody());
        }

    }

    @Test
    public void testThreeServerInRocksdbWithAsyncRequests() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        DLedgerServer dLedgerServer0 = launchServer(group, peers, "n0", "n1", DLedgerConfig.ROCKSDB);
        DLedgerServer dLedgerServer1 = launchServer(group, peers, "n1", "n1", DLedgerConfig.ROCKSDB);
        DLedgerServer dLedgerServer2 = launchServer(group, peers, "n2", "n1", DLedgerConfig.ROCKSDB);
        List<CompletableFuture<AppendEntryResponse>> futures = new ArrayList<>();
        long timestamp = System.currentTimeMillis()/1000;
        for (int i = 0; i < 10; i++) {
            AppendEntryRequest request = new AppendEntryRequest();
            request.setGroup(group);
            request.setRemoteId(dLedgerServer1.getMemberState().getSelfId());
            request.setTimestamp(timestamp);
            request.setBody(("testThreeServerInRocksdbWithAsyncRequests" + i).getBytes());
            futures.add(dLedgerServer1.handleAppend(request));
        }
        Thread.sleep(500);
        Assert.assertEquals(9, dLedgerServer0.getdLedgerStore().getLedgerEndIndex());
        Assert.assertEquals(9, dLedgerServer1.getdLedgerStore().getLedgerEndIndex());
        Assert.assertEquals(9, dLedgerServer2.getdLedgerStore().getLedgerEndIndex());

        DLedgerClient dLedgerClient = launchClient(group, peers);
        for (int i = 0; i < futures.size(); i++) {
            CompletableFuture<AppendEntryResponse> future = futures.get(i);
            Assert.assertTrue(future.isDone());
            Assert.assertEquals(i, future.get().getIndex());
            Assert.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), future.get().getCode());

            GetEntriesResponse getEntriesResponse = dLedgerClient.get(i);
            DLedgerEntry entry = getEntriesResponse.getEntries().get(0);
            Assert.assertEquals(1, getEntriesResponse.getEntries().size());
            Assert.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
            Assert.assertArrayEquals(("testThreeServerInRocksdbWithAsyncRequests" + i).getBytes(), entry.getBody());
        }

        GetListEntriesResponse getEntriesResponse = dLedgerClient.getByTime(timestamp);
        Assert.assertEquals(10, getEntriesResponse.getEntries().size());
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(i, getEntriesResponse.getEntries().get(i).getIndex());
            Assert.assertArrayEquals(("testThreeServerInRocksdbWithAsyncRequests" + i).getBytes(), getEntriesResponse.getEntries().get(i).getBody());
        }
    }
}
