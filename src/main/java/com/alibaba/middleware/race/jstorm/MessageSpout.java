package com.alibaba.middleware.race.jstorm;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.rocketmq.ConsumerFactory;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

abstract public class MessageSpout implements IRichSpout, MessageListenerConcurrently {
    private static final Logger LOG = LoggerFactory.getLogger(MessageSpout.class);
    protected SpoutOutputCollector collector;
    protected String topic;
    protected transient DefaultMQPushConsumer mqConsumer;
    protected transient LinkedBlockingDeque<Values> sendingQueue;

    public MessageSpout(String topic) {
        this.topic = topic;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        sendingQueue = new LinkedBlockingDeque<Values>(20);
        try {
            mqConsumer = ConsumerFactory.mkInstance(topic, this);
        } catch (Exception e) {
            LOG.error("%%%%%%: Failed to create RocketMq Consumer " + e);
            throw new RuntimeException("Failed to create RocketMq Consumer" + topic, e);
        }
        LOG.info("%%%%%%: Message Spout open success. Topic:" + topic);
    }

    @Override
    public void nextTuple() {
        Values message = null;
        try {
            //LOG.info("%%%%%%: Wait for message in next Tuple:" + topic);
            message = sendingQueue.take();
        } catch (InterruptedException e) {
            LOG.info("Failed to blocking the nextTuple.");
        }
        if (message != null) {
            // LOG.info("%%%%%%: Take succeed :" + topic);
            collector.emit(message);
        }else {
            LOG.info("%%%%%%: Take failed :" + topic);
        }
    }

    abstract public void putMessage(byte[] body);

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgList, ConsumeConcurrentlyContext context) {
        try {
            for (MessageExt msg : msgList) {
                byte [] body = msg.getBody();
                if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                    LOG.info("%%%%%%: Got the end signal");
                    continue;
                }
                putMessage(body);
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        } catch (Exception e) {
            LOG.error("Failed to emit. ", e);
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
    }

    @Override
    public void ack(Object msgId) {
        LOG.warn("%%%%%%: Shouldn't go this function");
    }

    @Override
    public void fail(Object msgId) {
        LOG.warn("%%%%%%: Shouldn't go this function");
    }

    @Override
    public void close() {
        if (mqConsumer != null) {
            mqConsumer.shutdown();
        }
    }

    @Override
    public void activate() {
        if (mqConsumer != null) {
            mqConsumer.resume();
        }
    }

    @Override
    public void deactivate() {
        if (mqConsumer != null) {
            mqConsumer.suspend();
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
