package com.sunland.rocketmq.utils;

import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;

public class MsgUtils {
    public static void clearExtralProperties(MessageExt msgExt) {
        MessageAccessor.clearProperty(msgExt, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        MessageAccessor.clearProperty(msgExt, MessageConst.PROPERTY_DELAY_TIME);
        MessageAccessor.clearProperty(msgExt, MessageConst.PROPERTY_REAL_QUEUE_ID);
        MessageAccessor.clearProperty(msgExt, MessageConst.PROPERTY_MIN_OFFSET);
        MessageAccessor.clearProperty(msgExt, MessageConst.PROPERTY_MAX_OFFSET);
        MessageAccessor.clearProperty(msgExt, MessageConst.PROPERTY_CONSUME_START_TIMESTAMP);
        MessageAccessor.clearProperty(msgExt, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);

    }
}
