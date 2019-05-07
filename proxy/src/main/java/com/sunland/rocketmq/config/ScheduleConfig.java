package com.sunland.rocketmq.config;

public class ScheduleConfig {

    private int pullThreadNum;
    private String group;
    private String peers;
    private String seekTimePath;
    private int pullBatchNum;

    private int flushCheckInterval;
    private int flushSeekTimeInterval = 1000;
    private int sendMsgRepeatedNum = 10;

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

}
