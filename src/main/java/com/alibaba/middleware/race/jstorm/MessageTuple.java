package com.alibaba.middleware.race.jstorm;

import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageTuple implements Serializable {

    protected final List<MessageExt> msgList;
    protected final MessageQueue mq;
    protected final AtomicInteger failureCounts;
    protected transient CountDownLatch latch;
    protected transient boolean isSuccess;
    protected final long createMs;
    protected long emitMs;

    public MessageTuple(List<MessageExt> msgList, MessageQueue mq) {
        this.msgList = msgList;
        this.mq = mq;
        this.failureCounts = new AtomicInteger(0);
        this.latch = new CountDownLatch(1);
        this.isSuccess = false;
        this.createMs = System.currentTimeMillis();
    }

    public AtomicInteger getFailureCounts() {
        return failureCounts;
    }

    public List<MessageExt> getMsgList() {
        return msgList;
    }

    public MessageQueue getMq() {
        return mq;
    }

    public long getCreateMs() {
        return createMs;
    }

    public long getEmitMs() {
        return emitMs;
    }

    public void updateEmitMs() {
        this.emitMs = System.currentTimeMillis();
    }

    public boolean waitFinish() throws InterruptedException {
        return latch.await(20, TimeUnit.MINUTES);
    }

    public void done() {
        isSuccess = true;
        latch.countDown();
    }

    public void fail() {
        isSuccess = false;
        latch.countDown();
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
