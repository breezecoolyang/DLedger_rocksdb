package com.sunland.rocketmq.config;

import java.io.File;

public class ScheduleConfig {

    private int pullThreadNum = 4;
    private String group = "default";
    private String peers = "n0-172.16.116.51:40911;n1-172.16.116.51:40912;n2-172.16.116.51:40913";
    private String seekTimePath = File.separator + "tmp" + File.separator + "proxy";
    private int pullBatchNum = 1000;

    private int flushCheckInterval = 500;
    private int flushSeekTimeInterval = 100;
    private int sendMsgRepeatedNum = 10;
    private int consumeMessageBatchMaxSize = 32;

    private String nameServeAddr = "172.16.116.48:9876";

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getPeers() {
        return peers;
    }

    public void setPeers(String peers) {
        this.peers = peers;
    }

    public int getPullThreadNum() {
        return pullThreadNum;
    }

    public void setPullThreadNum(int pullThreadNum) {
        pullThreadNum = pullThreadNum;
    }

    public String getSeekTimePath() {
        return seekTimePath;
    }

    public void setSeekTimePath(String seekTimePath) {
        this.seekTimePath = seekTimePath;
    }

    public int getPullBatchNum() {
        return pullBatchNum;
    }

    public void setPullBatchNum(int pullBatchNum) {
        this.pullBatchNum = pullBatchNum;
    }

    public int getFlushCheckInterval() {
        return flushCheckInterval;
    }

    public void setFlushCheckInterval(int flushCheckInterval) {
        this.flushCheckInterval = flushCheckInterval;
    }

    public int getFlushSeekTimeInterval() {
        return flushSeekTimeInterval;
    }

    public void setFlushSeekTimeInterval(int flushSeekTimeInterval) {
        this.flushSeekTimeInterval = flushSeekTimeInterval;
    }

    public int getSendMsgRepeatedNum() {
        return sendMsgRepeatedNum;
    }

    public void setSendMsgRepeatedNum(int sendMsgRepeatedNum) {
        this.sendMsgRepeatedNum = sendMsgRepeatedNum;
    }

    public String getNameServeAddr() {
        return nameServeAddr;
    }

    public void setNameServeAddr(String nameServeAddr) {
        this.nameServeAddr = nameServeAddr;
    }


    public int getConsumeMessageBatchMaxSize() {
        return consumeMessageBatchMaxSize;
    }

    public void setConsumeMessageBatchMaxSize(int consumeMessageBatchMaxSize) {
        this.consumeMessageBatchMaxSize = consumeMessageBatchMaxSize;
    }


}
