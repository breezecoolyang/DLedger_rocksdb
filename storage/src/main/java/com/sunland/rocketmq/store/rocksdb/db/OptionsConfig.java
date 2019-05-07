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

package com.sunland.rocketmq.store.rocksdb.db;

import com.sunland.rocketmq.store.rocksdb.config.ConfigManager;
import com.sunland.rocketmq.store.rocksdb.config.DbConfig;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.WriteOptions;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.Filter;
import org.rocksdb.BloomFilter;
import org.rocksdb.LRUCache;
import org.rocksdb.util.SizeUnit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OptionsConfig {
    private final DBOptions dbOptions = new DBOptions();

    private final ReadOptions readOptions = new ReadOptions();

    private final WriteOptions writeOptionsSync = new WriteOptions();

    private final WriteOptions writeOptionsAsync = new WriteOptions();

    private final Filter bloomFilter = new BloomFilter();

    private final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();

    private final ColumnFamilyOptions cloumnFamilyOptionsDefault = new ColumnFamilyOptions();

    private final List<CompressionType> compressionTypes = new ArrayList<>();

    public void initialOptions(ConfigManager configManager) {

        DbConfig dbConfig = configManager.getConfig().getDbConfig();
        dbOptions
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMaxBackgroundFlushes(dbConfig.getMaxBackgroundFlushes())
                .setMaxBackgroundCompactions(dbConfig.getMaxBackgroundCompactions())
//                .setDelayedWriteRate(dbConfig.getDelayedWriteRate() * SizeUnit.MB)
                .setMaxOpenFiles(2048)
                .setRowCache(new LRUCache(1024 * SizeUnit.MB, 16, true, 5))
                .setMaxSubcompactions(dbConfig.getMaxSubcompactions());

        dbOptions.setBaseBackgroundCompactions(dbConfig.getBaseBackgroundCompactions());

        readOptions
                .setPrefixSameAsStart(true);

        writeOptionsSync
                .setSync(true);

        writeOptionsAsync
                .setSync(false);

        // https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
        // Bloom filters are always kept in memory for open files,
        // unless BlockBasedTableOptions::cache_index_and_filter_blocks is set to true.
        // Number of open files is controlled by max_open_files option.
        blockBasedTableConfig
                .setFilter(bloomFilter)
                .setCacheIndexAndFilterBlocks(true)
                .setPinL0FilterAndIndexBlocksInCache(true);


        compressionTypes.addAll(Arrays.asList(
                CompressionType.NO_COMPRESSION, CompressionType.NO_COMPRESSION,
                CompressionType.NO_COMPRESSION, CompressionType.NO_COMPRESSION, CompressionType.NO_COMPRESSION,
                CompressionType.NO_COMPRESSION, CompressionType.NO_COMPRESSION)
        );
//        compressionTypes.addAll(Arrays.asList(
//                CompressionType.NO_COMPRESSION, CompressionType.NO_COMPRESSION,
//                CompressionType.LZ4_COMPRESSION, CompressionType.LZ4_COMPRESSION, CompressionType.LZ4_COMPRESSION,
//                CompressionType.ZSTD_COMPRESSION, CompressionType.ZSTD_COMPRESSION)
//        );


        cloumnFamilyOptionsDefault
                .setTableFormatConfig(blockBasedTableConfig)
                .useFixedLengthPrefixExtractor(10)
                .setWriteBufferSize(dbConfig.getWriteBufferSize() * SizeUnit.MB)
                .setMaxWriteBufferNumber(dbConfig.getMaxWriteBufferNumber())
                .setLevel0SlowdownWritesTrigger(dbConfig.getLevel0SlowdownWritesTrigger())
                .setLevel0StopWritesTrigger(dbConfig.getLevel0StopWritesTrigger())
                .setCompressionPerLevel(compressionTypes)
                .setTargetFileSizeBase(dbConfig.getTargetFileSizeBase() * SizeUnit.MB)
                .setMaxBytesForLevelBase(dbConfig.getMaxBytesForLevelBase() * SizeUnit.MB)
                .setOptimizeFiltersForHits(true);
    }

    public ColumnFamilyOptions getColumnFamilyOptions() {
        return cloumnFamilyOptionsDefault;
    }

    public DBOptions getDBOptions() {
        return dbOptions;
    }

    public WriteOptions getSyncWriteOptions() {
        return writeOptionsSync;
    }

    public WriteOptions getAsyncWriteOptions() {
        return writeOptionsAsync;
    }

    public ReadOptions getReadOptions() {
        return readOptions;
    }

    public void close() {
        dbOptions.close();
        writeOptionsSync.close();
        writeOptionsAsync.close();
        readOptions.close();
        cloumnFamilyOptionsDefault.close();
    }

}
