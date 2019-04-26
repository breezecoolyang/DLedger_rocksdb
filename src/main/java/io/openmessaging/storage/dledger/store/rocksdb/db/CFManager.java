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
import org.rocksdb.AbstractImmutableNativeReference;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;

import java.util.ArrayList;
import java.util.List;

public class CFManager {
    public ColumnFamilyHandle cfhDefault;

    private final List<ColumnFamilyDescriptor> cfDescriptor = new ArrayList<>();
    private final List<ColumnFamilyHandle> cfhandles = new ArrayList<>();


    public CFManager(OptionsConfig optionsConfig) {
        cfDescriptor.add(new ColumnFamilyDescriptor(CFHandlerNames.DEFAULT.getName().getBytes(Charsets.UTF_8), optionsConfig.getColumnFamilyOptions()));
    }


    public void initCFManger() {
        cfhDefault = cfhandles.get(CFHandlerNames.DEFAULT.ordinal());
    }

    public List<ColumnFamilyDescriptor> getCfDescriptor() {
        return cfDescriptor;
    }

    public List<ColumnFamilyHandle> getCfHandles() {
        return cfhandles;
    }

    public ColumnFamilyHandle getDefaultColumnFamilyHandle() {
        return cfhDefault;
    }

    public void closeHandles() {
        cfhandles.forEach(AbstractImmutableNativeReference::close);
    }
}
