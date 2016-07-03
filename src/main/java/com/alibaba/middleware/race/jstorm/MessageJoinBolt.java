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
    private long pay_count = 0L;
    private long order_count = 0L;
    private double pay_amount_count = 0.0;
    private double order_amount_count = 0.0;
    private long join_out_count = 0L;

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
                pay_count = pay_count+1;
                pay_amount_count += tuple.getDouble(1);
                long orderId = tuple.getLong(0);
                Payment payment = new Payment(tuple.getDouble(1), tuple.getBoolean(2), tuple.getLong(3));
                if (orderCache.containsKey(orderId)) {
                    Order order = orderCache.get(orderId);
                    processPayment(payment, order);
                    if (Math.abs(order.amount-payment.amount) < 0.0001) {
                        orderCache.remove(orderId);
                    } else if (order.amount > payment.amount) {
                        order.amount = order.amount - payment.amount;
                        orderCache.put(orderId, order);
                    } else {
                        LOG.error("%%%%%% 1: order amount :" + order.amount
                                + " less than payment amount: " + payment.amount);
                    }
                } else {
                    paymentCache.put(orderId, payment);
                }
            } else {
                order_count = order_count+1;
                order_amount_count += tuple.getDouble(1);
                long orderId = tuple.getLong(0);
                Order order = new Order(tuple.getDouble(1), tuple.getBoolean(2));
                if (paymentCache.containsKey(orderId)) {
                    Payment payment = paymentCache.get(orderId);
                    processPayment(payment, order);
                    if (Math.abs(order.amount - payment.amount) < 0.0001) {
                        paymentCache.remove(orderId);
                    }else if (order.amount > payment.amount) {
                        order.amount = order.amount - payment.amount;
                        orderCache.put(orderId, order);
                        paymentCache.remove(orderId);
                    } else {
                        LOG.error("%%%%%% 2: order amount :" + order.amount
                                + " less than payment amount: " + payment.amount);
                    }
                } else {
                    orderCache.put(orderId, order);
                }
            }
            // collector.ack(tuple);
        }catch (Exception e) {
            LOG.error("%%%%%%: Bolt execute failed " + e);
        }
    }

    void processPayment(Payment payment, Order order) {
        collector.emit(new Values(payment.amount, payment.minute, payment.platform, order.platform));
        join_out_count ++;
    }

    @Override
    public void cleanup() {
        LOG.info("%%%%%% cleanup. order cache size: " + orderCache.size());
        LOG.info("%%%%%% cleanup. payment cache size: " + paymentCache.size());
        LOG.info("%%%%%% payment count:" + pay_count +", order count:: " + order_count);
        LOG.info("%%%%%% payment amount count:" + pay_amount_count +", order amount count:: " + order_amount_count);
        LOG.info("%%%%%% join out count:" + join_out_count);
        for(Long key: paymentCache.keySet()) {
            LOG.info("%%%%%% payment Cache: " + key +", " + paymentCache.get(key).amount);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("payAmount", "minute", "payPlatform", "orderPlatform"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}