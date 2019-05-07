package com.sunland.rocketmq.model;

import com.alibaba.fastjson.annotation.JSONField;
import com.sunland.rocketmq.utils.JsonUtils;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.HashMap;
import java.util.Map;


public class InternalValue {
    @JSONField(name = "a")
    private String topic;

    @JSONField(name = "b")
    private String body;

    @JSONField(name = "c")
    private long createTime;

    @JSONField(name = "d")
    private String tags;

    @JSONField(name = "e")
    private Map<String, String> properties;

    public InternalValue() {
    }

    public InternalValue(MessageExt msgExt) {
        this.topic = msgExt.getTopic();
        this.body = new String(msgExt.getBody());
        this.tags = msgExt.getTags();
        this.createTime = System.currentTimeMillis();
        this.properties = msgExt.getProperties();
    }

    public String getTopic() {
        return topic;
    }

    public InternalValue setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public String getBody() {
        return body;
    }

    public InternalValue setBody(String body) {
        this.body = body;
        return this;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String toJsonString() {
        return JsonUtils.toJsonString(this);
    }

    @Override
    public String toString() {
        return "InternalValue{" +
                "topic='" + topic + '\'' +
                ", body='" + body + '\'' +
                ", createTime=" + createTime +
                ", tags='" + tags + '\'' +
                ", properties=" + properties +
                '}';
    }

    public static void main(String[] args) {
        InternalValue internalValue = new InternalValue();
        internalValue.setTopic("hello");
        internalValue.setBody("world");
        internalValue.setCreateTime(System.currentTimeMillis());
        internalValue.setTags("tags");

        Map<String, String> m = new HashMap<>();
        m.put("age", "30");
        m.put("name", "joy");
        internalValue.setProperties(m);

        String jsonString = internalValue.toJsonString();
        System.out.printf("%s", jsonString);

        final InternalValue internalValue2 = JsonUtils.fromJsonString(jsonString, InternalValue.class);
        System.out.printf("%s", internalValue2.toString());
    }
}