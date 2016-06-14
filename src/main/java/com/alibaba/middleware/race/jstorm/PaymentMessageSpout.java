package com.alibaba.middleware.race.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
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


public class PaymentMessageSpout implements IRichSpout, MessageListenerConcurrently {
    private static Logger LOG = LoggerFactory.getLogger(PaymentMessageSpout.class);
    SpoutOutputCollector _collector;
    DefaultMQPushConsumer _mqConsumer;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        try {
            _mqConsumer = ConsumerFactory.mkInstance(RaceConfig.MqPayTopic, this);
        } catch (Exception e) {
            LOG.error("Failed to create Meta Consumer ", e);
            throw new RuntimeException("Failed to create MetaConsumer" + RaceConfig.MqPayTopic, e);
        }
    }

    @Override
    public void nextTuple() {

    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list,
                                                    ConsumeConcurrentlyContext Context) {
        for (MessageExt msg : list) {
            byte [] body = msg.getBody();
            if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                //Info: 生产者停止生成数据, 并不意味着马上结束
                LOG.info("%%%%%%: Got the end signal");
            }

            PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
            _collector.emit(new Values(paymentMessage.getOrderId(), paymentMessage.getPayAmount(),
                    paymentMessage.getPayPlatform(),
                    RaceUtils.millisTosecondsOfMinute(paymentMessage.getCreateTime())));
            //LOG.info("%%%%%%: " + paymentMessage);
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("orderId", "payAmount", "payPlatform", "createTime"));
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void close() {
        if (_mqConsumer != null) {
            _mqConsumer.shutdown();
        }
    }

    @Override
    public void activate() {
        if (_mqConsumer != null) {
            _mqConsumer.resume();
        }
    }

    @Override
    public void deactivate() {
        if (_mqConsumer != null) {
            _mqConsumer.suspend();
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}