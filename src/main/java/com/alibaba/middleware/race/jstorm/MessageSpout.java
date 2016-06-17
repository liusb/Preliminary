package com.alibaba.middleware.race.jstorm;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.metric.MetricClient;
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
import java.util.concurrent.atomic.AtomicInteger;

public class MessageSpout implements IRichSpout, IAckValueSpout, IFailValueSpout,
        MessageListenerConcurrently {
    private static final Logger LOG = LoggerFactory.getLogger(MessageSpout.class);
    protected SpoutOutputCollector collector;
    protected String topic;
    protected transient DefaultMQPushConsumer mqConsumer;
    protected transient LinkedBlockingDeque<MessageTuple> sendingQueue;
    protected transient MetricClient metricClient;
    protected transient AsmHistogram processHistogram;
    protected transient AsmHistogram waitHistogram;

    public MessageSpout(String topic) {
        this.topic = topic;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        sendingQueue = new LinkedBlockingDeque<MessageTuple>();

        try {
            mqConsumer = ConsumerFactory.mkInstance(topic, this);
        } catch (Exception e) {
            LOG.error("%%%%%%: Failed to create RocketMq Consumer " + e);
            throw new RuntimeException("Failed to create RocketMq Consumer" + topic, e);
        }

        metricClient = new MetricClient(context);
        processHistogram = metricClient.registerHistogram("Histogram_Process_"+ topic);
        waitHistogram = metricClient.registerHistogram("Histogram_Wait_"+ topic);
        LOG.info("%%%%%%: Message Spout open success. Topic:" + topic);
    }

    @Override
    public void nextTuple() {
        MessageTuple messageTuple = null;
        try {
            LOG.info("%%%%%%: Wait for message in next Tuple:" + topic);
            messageTuple = sendingQueue.take();
        } catch (InterruptedException e) {
            LOG.info("Failed to blocking the nextTuple.");
        }
        if (messageTuple != null) {
            LOG.info("%%%%%%: Take succeed :" + topic);
            messageTuple.updateEmitMs();
            collector.emit(new Values(messageTuple), messageTuple.getCreateMs());
        }else {
            LOG.info("%%%%%%: Take failed :" + topic);
        }
    }

    @Override
    public void fail(Object msgId, List<Object> values) {
        MessageTuple messageTuple = (MessageTuple) values.get(0);
        AtomicInteger failTimes = messageTuple.getFailureCounts();
        int failNum = failTimes.incrementAndGet();
        if (failNum < 3) {
            sendingQueue.offer(messageTuple);
        }else {
            messageTuple.fail();
            LOG.warn("Message " + messageTuple.getMq() + " fail times " + failNum);
        }
    }

    @Override
    public void ack(Object msgId, List<Object> values) {
        MessageTuple messageTuple = (MessageTuple)values.get(0);
        waitHistogram.update(messageTuple.getEmitMs() - messageTuple.getCreateMs());
        processHistogram.update(System.currentTimeMillis() - messageTuple.getEmitMs());
        messageTuple.done();
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgList,
                                                    ConsumeConcurrentlyContext context) {
        try {
            MessageTuple messageTuple = new MessageTuple(msgList, context.getMessageQueue());
            sendingQueue.offer(messageTuple);
            LOG.info("%%%%%%: Message offered.");
            messageTuple.waitFinish();
            LOG.info("%%%%%%: Wait finished.");
            if (messageTuple.isSuccess()) {
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            } else {
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        } catch (Exception e) {
            LOG.error("Failed to emit. ", e);
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("MessageTuple"));
    }

    @Deprecated
    public void ack(Object msgId) {
        LOG.warn("%%%%%%: Shouldn't go this function");
    }

    @Deprecated
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
