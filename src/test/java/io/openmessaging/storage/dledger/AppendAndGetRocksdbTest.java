package io.openmessaging.storage.dledger;

import io.openmessaging.storage.dledger.client.DLedgerClient;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.GetEntriesResponse;
import io.openmessaging.storage.dledger.protocol.GetListEntriesResponse;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

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
}
