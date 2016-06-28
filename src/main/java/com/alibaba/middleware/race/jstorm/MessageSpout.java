package com.alibaba.middleware.race.jstorm;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
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

public class MessageSpout implements IRichSpout, MessageListenerConcurrently {
    private static final Logger LOG = LoggerFactory.getLogger(MessageSpout.class);
    protected SpoutOutputCollector collector;
    protected transient DefaultMQPushConsumer mqConsumer;
    protected transient LinkedBlockingDeque<Values> sendingQueue;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        sendingQueue = new LinkedBlockingDeque<Values>(30);
        try {
            mqConsumer = ConsumerFactory.mkInstance(this);
        } catch (Exception e) {
            LOG.error("%%%%%%: Failed to create RocketMq Consumer " + e);
            throw new RuntimeException("Failed to create RocketMq Consumer.", e);
        }
        LOG.info("%%%%%%: Message Spout open success.");
    }

    @Override
    public void nextTuple() {
        Values message = null;
        try {
            //LOG.info("%%%%%%: Wait for message in next Tuple:");
            message = sendingQueue.take();
        } catch (InterruptedException e) {
            LOG.info("Failed to blocking the nextTuple.");
        }
        if (message != null) {
            // LOG.info("%%%%%%: Take succeed :");
            collector.emit(message);
        }else {
            LOG.info("%%%%%%: Take failed :");
        }
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgList, ConsumeConcurrentlyContext context) {
        try {
            for (MessageExt msg : msgList) {
                byte [] body = msg.getBody();
                if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                    LOG.info("%%%%%%: Got the end signal");
                    continue;
                }
                if(msg.getTopic().equals(RaceConfig.MqPayTopic)) {
                    putPayMessage(body);
                }else {
                    putOrderMessage(body, msg.getTopic().equals(RaceConfig.MqTmallTradeTopic));
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        } catch (Exception e) {
            LOG.error("Failed to emit. ", e);
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
    }

    public void putPayMessage(byte[] body) {
        PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
        while (true) {
            try {
                sendingQueue.put(new Values(paymentMessage.getOrderId(), paymentMessage.getPayAmount(),
                        paymentMessage.getPayPlatform() == 0,
                        RaceUtils.millisToSecondsOfMinute(paymentMessage.getCreateTime())));
                break;
            } catch (Exception e) {
                LOG.info("Failed to blocking the putPayMessage.");
            }
        }
    }

    public void putOrderMessage(byte[] body, boolean platform) {
        OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
        while (true) {
            try {
                sendingQueue.put(new Values(orderMessage.getOrderId(), orderMessage.getTotalPrice(), platform, 0L));
                break;
            } catch (Exception e) {
                LOG.info("Failed to blocking the putPayMessage.");
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("orderId", "amount", "platform", "minute"));
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
