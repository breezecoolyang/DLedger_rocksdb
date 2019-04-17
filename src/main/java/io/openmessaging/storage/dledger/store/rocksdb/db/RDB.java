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
import io.openmessaging.storage.dledger.store.rocksdb.utils.FileIOUtils;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.WriteBatch;
import org.rocksdb.RocksIterator;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.AbstractImmutableNativeReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static io.openmessaging.storage.dledger.store.rocksdb.db.CFManager.CF_DESCRIPTOR;
import static io.openmessaging.storage.dledger.store.rocksdb.db.CFManager.CF_HANDLES;
import static io.openmessaging.storage.dledger.store.rocksdb.db.CFManager.initCFManger;

public class RDB {
    private static Logger logger = LoggerFactory.getLogger(RDB.class);

    static RocksDB db;

    public static void init(final String dbPath) {
        try {
            final long start = System.currentTimeMillis();
            boolean result = FileIOUtils.createOrExistsDir(new File(dbPath));
            assert result;


            db = RocksDB.open(OptionsConfig.DB_OPTIONS, dbPath, CF_DESCRIPTOR, CF_HANDLES);
            assert db != null;

            initCFManger(CF_HANDLES);

            final long cost = System.currentTimeMillis() - start;
            logger.info("succ open rocksdb, path:{}, cost:{}ms", dbPath, cost);
        } catch (RocksDBException e) {
            logger.error("error while open rocksdb, path:{}, err:{}", dbPath, e.getMessage(), e);
        }
    }

    public static boolean writeSync(final WriteBatch writeBatch) {
        return write(OptionsConfig.WRITE_OPTIONS_SYNC, writeBatch);
    }

    public static boolean writeAsync(final WriteBatch writeBatch) {
        return write(OptionsConfig.WRITE_OPTIONS_ASYNC, writeBatch);
    }

    private static boolean write(final WriteOptions writeOptions, final WriteBatch writeBatch) {
        try {
            db.write(writeOptions, writeBatch);
            logger.debug("succ write writeBatch, size:{}", writeBatch.count());
        } catch (RocksDBException e) {
            logger.error("error while write batch, err:{}", e.getMessage(), e);
            return false;
        }
        return true;
    }

    public static RocksIterator newIterator(final ColumnFamilyHandle cfHandle) {
        return db.newIterator(cfHandle, OptionsConfig.READ_OPTIONS);
    }

    public static boolean deleteFilesInRange(final ColumnFamilyHandle cfh, final byte[] beginKey,
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

    public static boolean delete(final ColumnFamilyHandle cfh, final byte[] key) {
        try {
            db.delete(cfh, key);
            logger.debug("succ delete key, columnFamilyHandle:{}, key:{}", cfh.toString(), new String(key));
        } catch (RocksDBException e) {
            logger.error("error while delete key, columnFamilyHandle:{}, key:{}, err:{}",
                    cfh.toString(), new String(key), e.getMessage(), e);
        }
        return true;
    }

    public static byte[] get(final ColumnFamilyHandle cfh, final byte[] key) {
        try {
            return db.get(cfh, key);
        } catch (RocksDBException e) {
            logger.error("error while get, columnFamilyHandle:{}, key:{}, err:{}",
                    cfh.toString(), new String(key), e.getMessage(), e);
            return null;
        }
    }

    public static boolean put(final ColumnFamilyHandle cfh, final byte[] key, final byte[] value) {
        try {
            db.put(cfh, key, value);
        } catch (RocksDBException e) {
            logger.error("error while put, columnFamilyHandle:{}, key:{}, err:{}",
                    cfh.isOwningHandle(), new String(key), e.getMessage(), e);
            return false;
        }
        return true;
    }

    public static boolean put(final ColumnFamilyHandle cfh, WriteOptions writeOptions, final byte[] key, final byte[] value) {
        try {
            db.put(cfh, writeOptions, key, value);
        } catch (RocksDBException e) {
            logger.error("error while put, columnFamilyHandle:{}, key:{}, err:{}",
                    cfh.isOwningHandle(), new String(key), e.getMessage(), e);
            return false;
        }
        return true;
    }

    public static boolean putSync(final ColumnFamilyHandle cfh, final byte[] key, final byte[] value) {
        return put(cfh, OptionsConfig.WRITE_OPTIONS_SYNC, key, value);
    }

    public static ColumnFamilyHandle createCF(final String name) {
        try {
            ColumnFamilyHandle cfh = db.createColumnFamily(new ColumnFamilyDescriptor(name.getBytes(Charsets.UTF_8)));
            return cfh;
        } catch (RocksDBException e) {
            logger.error("error while createCF, msg:{}", e.getMessage(), e);
            return null;
        }
    }

    public static boolean dropCF(final ColumnFamilyHandle cfh) {
        try {
            db.dropColumnFamily(cfh);
            return true;
        } catch (RocksDBException e) {
            logger.error("error while dropCF, msg:{}", e.getMessage(), e);
            return false;
        }
    }

    public static void close() {
        OptionsConfig.DB_OPTIONS.close();
        OptionsConfig.WRITE_OPTIONS_SYNC.close();
        OptionsConfig.WRITE_OPTIONS_ASYNC.close();
        OptionsConfig.READ_OPTIONS.close();
        OptionsConfig.COLUMN_FAMILY_OPTIONS_DEFAULT.close();
        CF_HANDLES.forEach(AbstractImmutableNativeReference::close);
        if (db != null) {
            db.close();
        }
    }
}