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

import com.google.common.base.Charsets;
import io.openmessaging.storage.dledger.store.rocksdb.config.ConfigManager;
import io.openmessaging.storage.dledger.store.rocksdb.utils.FileIOUtils;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.WriteBatch;
import org.rocksdb.RocksIterator;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.FlushOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;


public class RDB {
    private static Logger logger = LoggerFactory.getLogger(RDB.class);

    private RocksDB db;

    private String dbPath;

    private OptionsConfig optionsConfig;

    private CFManager cfManager;

    private ColumnFamilyHandle cfHandle;

    private ConfigManager configManager;

    public RDB(ConfigManager configManager) {
        optionsConfig = new OptionsConfig();
        this.configManager = configManager;
        optionsConfig.initialOptions(this.configManager);
        cfManager = new CFManager(optionsConfig);

    }

    public void init() {
        try {
            final String dbPath = configManager.getConfig().getDbConfig().getDbPath();
            final long start = System.currentTimeMillis();
            boolean result = FileIOUtils.createOrExistsDir(new File(dbPath));
            assert result;

            db = RocksDB.open(optionsConfig.getDBOptions(), dbPath, cfManager.getCfDescriptor(), cfManager.getCfHandles());
            assert db != null;

            cfManager.initCFManger();

            cfHandle = cfManager.getDefaultColumnFamilyHandle();

            final long cost = System.currentTimeMillis() - start;
            this.dbPath = dbPath;
            logger.info("succ open rocksdb, path:{}, cost:{}ms", dbPath, cost);
        } catch (RocksDBException e) {
            logger.error("error while open rocksdb, path:{}, err:{}", dbPath, e.getMessage(), e);
        }
    }

    public boolean writeSync(final WriteBatch writeBatch) {
        return write(optionsConfig.getSyncWriteOptions(), writeBatch);
    }

    public boolean writeAsync(final WriteBatch writeBatch) {
        return write(optionsConfig.getAsyncWriteOptions(), writeBatch);
    }

    private boolean write(final WriteOptions writeOptions, final WriteBatch writeBatch) {
        try {
//            logger.error("write dbpath is [{}], db object is [{}]", dbPath, db);
            db.write(writeOptions, writeBatch);
            logger.debug("succ write writeBatch, size:{}", writeBatch.count());
        } catch (RocksDBException e) {
            logger.error("error while write batch, err:{}", e.getMessage(), e);
            return false;
        }
        return true;
    }

    public RocksIterator newIterator(final ColumnFamilyHandle cfHandle) {
        return db.newIterator(cfHandle, optionsConfig.getReadOptions());
    }

    public RocksIterator newIteratorDefault() {
        return newIterator(cfHandle);
    }

    public boolean deleteFilesInRange(final ColumnFamilyHandle cfh, final byte[] beginKey,
                                      final byte[] endKey) {
        try {
            db.deleteRange(cfh, beginKey, endKey);
            logger.debug("succ delete range, columnFamilyHandle:{}, beginKey:{}, endKey:{}",
                    cfh.toString(), new String(beginKey), new String(endKey));
        } catch (RocksDBException e) {
            logger.error("error while delete range, columnFamilyHandle:{}, beginKey:{}, endKey:{}, err:{}",
                    cfh.toString(), new String(beginKey), new String(endKey), e.getMessage(), e);
            return false;
        }
        return true;
    }

    public boolean delete(final ColumnFamilyHandle cfh, final byte[] key) {
        try {
            db.delete(cfh, key);
            logger.debug("succ delete key, columnFamilyHandle:{}, key:{}", cfh.toString(), new String(key));
        } catch (RocksDBException e) {
            logger.error("error while delete key, columnFamilyHandle:{}, key:{}, err:{}",
                    cfh.toString(), new String(key), e.getMessage(), e);
        }
        return true;
    }

    public boolean deleteDefault(final byte[] key) {
        return delete(cfHandle, key);
    }

    public byte[] get(final ColumnFamilyHandle cfh, final byte[] key) {
        try {
//            logger.error("get dbpath is [{}], db object is [{}]", dbPath, db);
            return db.get(cfh, key);
        } catch (RocksDBException e) {
            logger.error("error while get, columnFamilyHandle:{}, key:{}, err:{}",
                    cfh.toString(), new String(key), e.getMessage(), e);
            return null;
        }
    }

    public byte[] getDefault(final byte[] key) {
        return get(cfHandle, key);
    }

    public boolean put(final ColumnFamilyHandle cfh, final byte[] key, final byte[] value) {
        try {
            db.put(cfh, key, value);
        } catch (RocksDBException e) {
            logger.error("error while put, columnFamilyHandle:{}, key:{}, err:{}",
                    cfh.isOwningHandle(), new String(key), e.getMessage(), e);
            return false;
        }
        return true;
    }

    public boolean put(final ColumnFamilyHandle cfh, WriteOptions writeOptions, final byte[] key, final byte[] value) {
        try {
            db.put(cfh, writeOptions, key, value);
        } catch (RocksDBException e) {
            logger.error("error while put, columnFamilyHandle:{}, key:{}, err:{}",
                    cfh.isOwningHandle(), new String(key), e.getMessage(), e);
            return false;
        }
        return true;
    }


    public boolean putSync(final ColumnFamilyHandle cfh, final byte[] key, final byte[] value) {
        return put(cfh, optionsConfig.getSyncWriteOptions(), key, value);
    }

    public ColumnFamilyHandle createCF(final String name) {
        try {
            ColumnFamilyHandle cfh = db.createColumnFamily(new ColumnFamilyDescriptor(name.getBytes(Charsets.UTF_8)));
            return cfh;
        } catch (RocksDBException e) {
            logger.error("error while createCF, msg:{}", e.getMessage(), e);
            return null;
        }
    }

    public boolean dropCF(final ColumnFamilyHandle cfh) {
        try {
            db.dropColumnFamily(cfh);
            return true;
        } catch (RocksDBException e) {
            logger.error("error while dropCF, msg:{}", e.getMessage(), e);
            return false;
        }
    }

    public void flush() {
        try (
                final FlushOptions flushOptions = new FlushOptions()
                        .setWaitForFlush(true)) {
            db.flush(flushOptions);
        } catch (Exception e) {
            logger.error("error while flush to storage", e);
        }

    }

    public ColumnFamilyHandle getDefaultCfHandle() {
        return cfHandle;
    }

    public void close() {
        optionsConfig.close();
        cfManager.closeHandles();

        if (db != null) {
            db.close();
        }
    }
}