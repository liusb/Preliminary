package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class MessageJoinBolt implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(MessageJoinBolt.class);

    protected OutputCollector collector;
    protected transient HashMap<Long, Order> orderCache;
    protected transient HashMap<Long, Payment> paymentCache;

    class Order {
        double amount;
        boolean platform;
        Order(double amount, boolean platform) {
            this.amount = amount;
            this.platform = platform;
        }
    }

    class Payment {
        double amount;
        boolean platform;
        long minute;
        Payment(double amount, boolean platform, long minute) {
            this.amount = amount;
            this.platform = platform;
            this.minute = minute;
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        orderCache = new HashMap<Long, Order>();
        paymentCache = new HashMap<Long, Payment>();
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            if (tuple.getLong(3) != 0L) {
                long orderId = tuple.getLong(0);
                Payment payment = new Payment(tuple.getDouble(1), tuple.getBoolean(2), tuple.getLong(3));
                if (orderCache.containsKey(orderId)) {
                    Order order = orderCache.get(orderId);
                    processPayment(payment, order);
                    if (order.amount == payment.amount) {
                        orderCache.remove(orderId);
                    } else if (order.amount > payment.amount) {
                        order.amount = order.amount - payment.amount;
                        orderCache.put(orderId, order);
                    } else {
                        LOG.error("%%%%%%: order amount not equal to payment amount, order_id: " + orderId);
                    }
                } else {
                    paymentCache.put(orderId, payment);
                }
            } else {
                long orderId = tuple.getLong(0);
                Order order = new Order(tuple.getDouble(1), tuple.getBoolean(2));
                if (paymentCache.containsKey(orderId)) {
                    Payment payment = paymentCache.get(orderId);
                    processPayment(payment, order);
                    paymentCache.remove(orderId);
                    if (order.amount > payment.amount) {
                        order.amount = order.amount - payment.amount;
                        orderCache.put(orderId, order);
                    }
                } else {
                    orderCache.put(orderId, order);
                }
            }
        }catch (Exception e) {
            LOG.error("Bolt execute failed " + e);
        }
    }

    void processPayment(Payment payment, Order order) {
        collector.emit(new Values(payment.amount, payment.minute, payment.platform, order.platform));
    }

    @Override
    public void cleanup() { }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("payAmount", "minute", "payPlatform", "orderPlatform"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}