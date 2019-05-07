package com.sunland.rocketmq.db;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.Test;

public class DbOperationTest {

    @Test
    public void testMultiMapOperation() throws Exception {

        Multimap<Long, String> map = ArrayListMultimap.create();
        map.put(1L, "aaa");
        map.put(2L, "bbb");
        map.put(1L,"ccc");
        map.put(3L, "dddd");
        map.put(2L, "abced");



    }
}
