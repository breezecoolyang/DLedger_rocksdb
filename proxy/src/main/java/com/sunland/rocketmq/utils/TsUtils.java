package com.sunland.rocketmq.utils;

public class TsUtils {
    public static long genTS() {
        return System.currentTimeMillis() / 1000;
    }
}
