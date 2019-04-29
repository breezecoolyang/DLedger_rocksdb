package io.openmessaging.storage.dledger.cmdline;

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.Parameter;
import io.openmessaging.storage.dledger.client.DLedgerClient;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppendTPSCommand extends BaseCommand {
    private static Logger logger = LoggerFactory.getLogger(AppendCommand.class);

    @Parameter(names = {"--group", "-g"}, description = "Group of this server")
    private String group = "default";

    @Parameter(names = {"--peers", "-p"}, description = "Peer info of this server")
    private String peers = "n0-localhost:20911";

    @Parameter(names = {"--time", "-t"}, description = "the timestamp to append")
    private long timestamp = 1536811267;

    @Parameter(names = {"--data", "-d"}, description = "the data to append")
    private String data = "tasdfasdgasdfestfordb";

    @Parameter(names = {"--count", "-c"}, description = "append several times")
    private int count = 1;


    @Override
    public void doCommand() {
        DLedgerClient dLedgerClient = new DLedgerClient(group, peers);
        dLedgerClient.startup();
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            AppendEntryResponse response = dLedgerClient.append(timestamp, data.getBytes());
            if (response.getIndex() < 0) {
                logger.info("Append Error, Result:{}", JSON.toJSONString(response));
            }
        }
        long end = System.currentTimeMillis();
        logger.info("time cost:{}, test count is {}", end - start, count);
        dLedgerClient.shutdown();
    }
}
