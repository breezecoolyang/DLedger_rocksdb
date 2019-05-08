package com.sunland.rocketmq.db;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class DbOperationTest {

    @Test
    public void testMultiMapOperation() throws Exception {

        Multimap<Long, String> map = ArrayListMultimap.create();
        map.put(1L, "aaa");
        map.put(2L, "bbb");
        map.put(1L,"ccc");
        map.put(3L, "dddd");
        map.put(2L, "abced");
        map.put(1L, "kkk");
        Multimap<Long, String> deleteMap = ArrayListMultimap.create();
        deleteMap.put(1L, "aaa");
        deleteMap.put(3L, "dddd");

//        Collection<String> strValue = map.get(1L);
//        Assert.assertTrue(strValue.contains("aaa"));
//        Assert.assertTrue(strValue.contains("ccc"));
//        Assert.assertTrue(strValue.size() == 2);
//        strValue.remove("aaa");
//        Assert.assertTrue(strValue.size() == 1);

        System.out.printf("strkey is [%s] \n", map.keySet());
        System.out.printf("before delete map is [%s] \n",map);

        Assert.assertTrue(map.size() == 6);

        Iterator<Map.Entry<Long, String>> iterator = deleteMap.entries().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, String> entry = iterator.next();
            if (entry != null) {
                map.remove(entry.getKey(), entry.getValue());
            }
        }

        Assert.assertTrue(map.size() == 4);
        System.out.printf("after delete map is [%s] \n",map);




    }
}
