package com.sunland.rocketmq.model;


public class InternalKey {
    private static final String SEPARATOR = "-";
    private long timestamp;


    public InternalKey(long timestamp) {
        this.timestamp = timestamp;
    }

    public InternalKey(InternalKey internalKey) {
        this.timestamp = internalKey.getTimestamp();

    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "InternalKey{" +
                "timestamp=" + timestamp +
                '}';
    }
}