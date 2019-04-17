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

package io.openmessaging.storage.dledger.store.rocksdb.db;

import io.openmessaging.storage.dledger.store.rocksdb.config.ConfigManager;
import io.openmessaging.storage.dledger.store.rocksdb.config.DbConfig;
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
    static final DBOptions DB_OPTIONS = new DBOptions();

    static final ReadOptions READ_OPTIONS = new ReadOptions();

    static final WriteOptions WRITE_OPTIONS_SYNC = new WriteOptions();

    static final WriteOptions WRITE_OPTIONS_ASYNC = new WriteOptions();

    private static final Filter BLOOM_FILTER = new BloomFilter();

    private static final BlockBasedTableConfig BLOCK_BASED_TABLE_CONFIG = new BlockBasedTableConfig();

    static final ColumnFamilyOptions COLUMN_FAMILY_OPTIONS_DEFAULT = new ColumnFamilyOptions();

    private static final List<CompressionType> COMPRESSION_TYPES = new ArrayList<>();

    static {
        DbConfig dbConfig = ConfigManager.getConfig().getDbConfig();
        DB_OPTIONS
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMaxBackgroundFlushes(dbConfig.getMaxBackgroundFlushes())
                .setMaxBackgroundCompactions(dbConfig.getMaxBackgroundCompactions())
//                .setDelayedWriteRate(dbConfig.getDelayedWriteRate() * SizeUnit.MB)
                .setMaxOpenFiles(2048)
                .setRowCache(new LRUCache(1024 * SizeUnit.MB, 16, true, 5))
                .setMaxSubcompactions(dbConfig.getMaxSubcompactions());

        DB_OPTIONS.setBaseBackgroundCompactions(dbConfig.getBaseBackgroundCompactions());

        READ_OPTIONS
                .setPrefixSameAsStart(true);

        WRITE_OPTIONS_SYNC
                .setSync(true);

        WRITE_OPTIONS_ASYNC
                .setSync(false);

        // https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
        // Bloom filters are always kept in memory for open files,
        // unless BlockBasedTableOptions::cache_index_and_filter_blocks is set to true.
        // Number of open files is controlled by max_open_files option.
        BLOCK_BASED_TABLE_CONFIG
                .setFilter(BLOOM_FILTER)
                .setCacheIndexAndFilterBlocks(true)
                .setPinL0FilterAndIndexBlocksInCache(true);


//        COMPRESSION_TYPES.addAll(Arrays.asList(
//                CompressionType.NO_COMPRESSION, CompressionType.NO_COMPRESSION,
//                CompressionType.NO_COMPRESSION, CompressionType.NO_COMPRESSION, CompressionType.NO_COMPRESSION,
//                CompressionType.NO_COMPRESSION, CompressionType.NO_COMPRESSION)
//        );
        COMPRESSION_TYPES.addAll(Arrays.asList(
                CompressionType.NO_COMPRESSION, CompressionType.NO_COMPRESSION,
                CompressionType.LZ4_COMPRESSION, CompressionType.LZ4_COMPRESSION, CompressionType.LZ4_COMPRESSION,
                CompressionType.ZSTD_COMPRESSION, CompressionType.ZSTD_COMPRESSION)
        );


        COLUMN_FAMILY_OPTIONS_DEFAULT
                .setTableFormatConfig(BLOCK_BASED_TABLE_CONFIG)
                .useFixedLengthPrefixExtractor(10)
                .setWriteBufferSize(dbConfig.getWriteBufferSize() * SizeUnit.MB)
                .setMaxWriteBufferNumber(dbConfig.getMaxWriteBufferNumber())
                .setLevel0SlowdownWritesTrigger(dbConfig.getLevel0SlowdownWritesTrigger())
                .setLevel0StopWritesTrigger(dbConfig.getLevel0StopWritesTrigger())
                .setCompressionPerLevel(COMPRESSION_TYPES)
                .setTargetFileSizeBase(dbConfig.getTargetFileSizeBase() * SizeUnit.MB)
                .setMaxBytesForLevelBase(dbConfig.getMaxBytesForLevelBase() * SizeUnit.MB)
                .setOptimizeFiltersForHits(true);
    }
}
