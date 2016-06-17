package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;


public class AmountCalculateBolt implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(AmountCalculateBolt.class);

    protected String tairPrefix;
    protected OutputCollector collector;
    protected TairOperatorImpl tairClient;
    protected transient HashMap<Long, Double> orderCache;
    protected transient HashMap<Long, Payment> paymentCache;
    protected transient HashMap<Long, Double> slots;
    protected transient HashMap<Long, Double> updateSlots;
    protected transient Set<Long> updateKeys;
    double _currentAmount;
    long _currentMinute;

    class Payment {
        double amount;
        long createTime;
        Payment(double amount, long createTime) {
            this.amount = amount;
            this.createTime = createTime;
        }
    }

    public AmountCalculateBolt(String prefix) {
        tairPrefix = prefix;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        tairClient = new TairOperatorImpl();
        orderCache = new HashMap<Long, Double>();
        paymentCache = new HashMap<Long, Payment>();
        slots = new HashMap<Long, Double>();
        updateSlots = new HashMap<Long, Double>();
        updateKeys = new HashSet<Long>();
        _currentAmount = 0;
        _currentMinute = RaceUtils.millisToSecondsOfMinute(System.currentTimeMillis());
    }

    private void processPayment(Payment payment) {
        long minute = RaceUtils.millisToSecondsOfMinute(payment.createTime);
        updateKeys.add(minute);
        if (updateSlots.containsKey(minute)) {
            updateSlots.put(minute, updateSlots.get(minute) + payment.amount);
        }else {
            updateSlots.put(minute, payment.amount);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            updateKeys.clear();
            updateSlots.clear();

            String source = tuple.getSourceComponent();
            if (source.startsWith("pay")) {
                MessageTuple messageTuple = (MessageTuple)tuple.getValue(0);
                List<MessageExt> msgList = messageTuple.getMsgList();
                for (MessageExt msg : msgList) {
                    byte [] body = msg.getBody();
                    if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                        LOG.info("%%%%%%: Got the end signal");
                        continue;
                    }
                    PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
                    long orderId = paymentMessage.getOrderId();
                    Payment payment = new Payment(paymentMessage.getPayAmount(), paymentMessage.getCreateTime());
                    if (orderCache.containsKey(orderId)) {
                        processPayment(payment);
                        double orderAmount = orderCache.get(orderId);
                        if ((orderAmount - payment.amount < 0.001) && (orderAmount - payment.amount > -0.001)) {
                            orderCache.remove(orderId);
                        } else if (orderAmount > payment.amount) {
                            orderCache.put(orderId, orderAmount - payment.amount);
                        } else {
                            LOG.error("%%%%%%: order amount not equal to payment amount, order_id: " + orderId);
                        }
                    } else {
                        paymentCache.put(orderId, payment);
                    }
                }
            } else {
                MessageTuple messageTuple = (MessageTuple)tuple.getValue(0);
                List<MessageExt> msgList = messageTuple.getMsgList();
                for (MessageExt msg : msgList) {
                    byte[] body = msg.getBody();
                    if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                        LOG.info("%%%%%%: Got the end signal");
                        continue;
                    }
                    OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
                    long orderId = orderMessage.getOrderId();
                    double totalPrice = orderMessage.getTotalPrice();
                    if (paymentCache.containsKey(orderId)) {
                        Payment payment = paymentCache.get(orderId);
                        processPayment(payment);
                        paymentCache.remove(orderId);
                        if (totalPrice - payment.amount > 0.001) {
                            orderCache.put(orderId, totalPrice - payment.amount);
                        }
                    } else {
                        orderCache.put(orderId, totalPrice);
                    }
                }
            }

            for (long key : updateKeys){
                double amount = updateSlots.get(key);
                if (slots.containsKey(key)) {
                    amount += slots.get(key);
                }
                tairClient.write(tairPrefix + key, amount);
                slots.put(key, amount);
            }
            collector.ack(tuple);
        }catch (Exception e) {
            collector.fail(tuple);
            LOG.error("Bolt execute failed " + e);
        }
    }

    @Override
    public void cleanup() {
       tairClient.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}