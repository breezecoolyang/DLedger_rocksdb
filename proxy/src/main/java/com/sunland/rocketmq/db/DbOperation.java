package com.sunland.rocketmq.db;

import com.google.common.base.Charsets;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.sunland.rocketmq.client.DLedgerClient;
import com.sunland.rocketmq.config.ConfigManager;
import com.sunland.rocketmq.protocol.AppendEntryResponse;
import com.sunland.rocketmq.protocol.GetListEntriesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DbOperation {

    private static final Logger log = LoggerFactory.getLogger(DbOperation.class);
    private DLedgerClient client;

    public DbOperation() {
        client = new DLedgerClient(ConfigManager.getConfig().getGroup(), ConfigManager.getConfig().getPeers());
        client.startup();
    }

    public boolean append(long timestamp, byte[] body) {
        int count = 0;
        int sendMaxNum = ConfigManager.getConfig().getSendMsgRepeatedNum();
        while (count < sendMaxNum) {
            AppendEntryResponse response = client.append(timestamp, body);
            if (response.getIndex() >= 0) {
                return true;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(200);
            } catch (InterruptedException e) {
            }
            count++;
        }
        return false;
    }

    public void append(Multimap<Long, String> map) {

        Multimap<Long, String> deleteMap = ArrayListMultimap.create();
        for (long key : map.keySet()) {
            Collection<String> values = map.get(key);
            for (String strValue : values) {
                boolean result = append(key, strValue.getBytes(Charsets.UTF_8));
                if (result) {
                    deleteMap.put(key, strValue);
                }
                else {
                    log.error("send msg error,key:{},value:{}", key, strValue);
                }
            }
        }

        Iterator<Map.Entry<Long, String>> iterator = deleteMap.entries().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, String> entry = iterator.next();
            if (entry != null) {
                map.remove(entry.getKey(), entry.getValue());
            }
        }
    }

    public GetListEntriesResponse getByTime(long timestamp) {
        return client.getByTime(timestamp);
    }

    public void shutdown() {
        client.shutdown();
    }
}
