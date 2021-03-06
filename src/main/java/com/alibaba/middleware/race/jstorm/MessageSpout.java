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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

public class MessageSpout implements IRichSpout, MessageListenerConcurrently {
    private static final Logger LOG = LoggerFactory.getLogger(MessageSpout.class);
    protected SpoutOutputCollector collector;
    protected transient DefaultMQPushConsumer mqConsumer;
    protected transient LinkedBlockingDeque<ArrayList<Values>> sendingQueue;
//    private long pay_count = 0L;
//    private long order_count = 0L;
//    private double pay_amount_count = 0.0;
//    private double order_amount_count = 0.0;
//    private long ack_count = 0L;
//    private long fail_count = 0L;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        sendingQueue = new LinkedBlockingDeque<ArrayList<Values>>();
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
        ArrayList<Values> values = null;
        try {
            //LOG.info("%%%%%%: Wait for message in next Tuple.");
            values = sendingQueue.take();
        } catch (InterruptedException e) {
            LOG.info("Failed to blocking the nextTuple.");
        }
        if (values != null) {
            // LOG.info("%%%%%%: Take succeed.");
//            if((((Long) message.get(3))) != 0L) {
//                pay_count = pay_count+1;
//                pay_amount_count += (Double)message.get(1);
//            } else {
//                order_count = order_count+1;
//                order_amount_count += (Double)message.get(1);
//            }
            for(Values value: values) {
                collector.emit(value); //, pay_count+order_count);
            }
        }else {
            LOG.info("%%%%%%: Take failed.");
        }
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgList, ConsumeConcurrentlyContext context) {
        try {
            //ArrayList<Values> values = new ArrayList<Values>(msgList.size());
            String msgTopic = context.getMessageQueue().getTopic();
            if(msgTopic.equals(RaceConfig.MqPayTopic)) {
                for (MessageExt msg : msgList) {
                    byte[] body = msg.getBody();
                    if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                        LOG.info("%%%%%%: Got the end signal. topic: " + RaceConfig.MqPayTopic);
                        continue;
                    }
                    PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
                    collector.emit(new Values(paymentMessage.getOrderId(), paymentMessage.getPayAmount(),
                            paymentMessage.getPayPlatform() == 0, paymentMessage.getCreateTime()));
                }
            } else {
                boolean platform = msgTopic.equals(RaceConfig.MqTmallTradeTopic);
                for (MessageExt msg : msgList) {
                    byte[] body = msg.getBody();
                    if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                        LOG.info("%%%%%%: Got the end signal. topic: " + msgTopic);
                        continue;
                    }
                    OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
                    collector.emit(new Values(orderMessage.getOrderId(), orderMessage.getTotalPrice(), platform, 0L));
                }
            }
            //putMessage(values);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        } catch (Exception e) {
            LOG.error("Failed to emit. ", e);
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
    }

//    public void putMessage(ArrayList<Values> values) {
//        while (true) {
//            try {
//                sendingQueue.put(values);
//                break;
//            } catch (Exception e) {
//                LOG.info("Failed to blocking the putPayMessage.");
//            }
//        }
//    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("orderId", "amount", "platform", "createTime"));
    }

    @Override
    public void ack(Object msgId) {
        LOG.warn("%%%%%%: Shouldn't go this function");
//        ack_count ++;
    }

    @Override
    public void fail(Object msgId) {
        LOG.warn("%%%%%%: Shouldn't go this function");
//        fail_count ++;
    }

    @Override
    public void close() {
        if (mqConsumer != null) {
            mqConsumer.shutdown();
        }
//        LOG.info("%%%%%% payment count:" + pay_count +", order count:: " + order_count);
//        LOG.info("%%%%%% payment amount count:" + pay_amount_count +", order amount count:: " + order_amount_count);
//        LOG.info("%%%%%% ack count:" + ack_count +", fail count:: " + fail_count);
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
