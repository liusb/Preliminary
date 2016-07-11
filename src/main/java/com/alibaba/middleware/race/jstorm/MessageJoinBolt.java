package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;


public class MessageJoinBolt implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(MessageJoinBolt.class);

    protected OutputCollector collector;
    protected transient HashMap<Long, Order> orderCache;
    protected transient HashMap<Long, Payment> paymentCache;
    protected transient HashSet<Long> processedOrder;
//    private long pay_count = 0L;
//    private long order_count = 0L;
//    private double pay_amount_count = 0.0;
//    private double order_amount_count = 0.0;
//    private long join_out_count = 0L;

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
        long createTime;
        Payment next;
        Payment(double amount, boolean platform, long createTime) {
            this.amount = amount;
            this.platform = platform;
            this.createTime = createTime;
            this.next = null;
        }

        public boolean equals(Payment obj) {
            return this.amount == obj.amount && this.createTime == obj.createTime
                    && this.platform == obj.platform;
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        orderCache = new HashMap<Long, Order>(512);
        paymentCache = new HashMap<Long, Payment>(256);
        processedOrder = new HashSet<Long>(10000);
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            long orderId = tuple.getLong(0);
            if (tuple.getLong(3) != 0L) {
//                pay_count = pay_count+1;
//                pay_amount_count += tuple.getDouble(1);
                Payment payment = new Payment(tuple.getDouble(1), tuple.getBoolean(2), tuple.getLong(3));
                if(putPayment(orderId, payment)) {
                    if (orderCache.containsKey(orderId)) {
                        processPayment(orderId, payment);
                    }
                }
            } else {
//                order_count = order_count+1;
//                order_amount_count += tuple.getDouble(1);
                if(!processedOrder.contains(orderId)) {
                    processedOrder.add(orderId);
                    Order order = new Order(tuple.getDouble(1), tuple.getBoolean(2));
                    if (paymentCache.containsKey(orderId)) {
                        processOrder(orderId, order);
                    } else {
                        orderCache.put(orderId, order);
                    }
                }
            }
            // collector.ack(tuple);
        }catch (Exception e) {
            LOG.error("%%%%%%: Bolt execute failed " + e);
        }
    }

    private boolean putPayment(long orderId, Payment payment) {
        if (paymentCache.containsKey(orderId)) {
            Payment pre = paymentCache.get(orderId);
            while (true) {
                if (payment.equals(pre)) {
                    return false;
                }
                if(pre.next == null) {
                    pre.next = payment;
                    break;
                }
                pre = pre.next;
            }
        } else {
            paymentCache.put(orderId, payment);
        }
        return true;
    }

    void processPayment(long orderId, Payment payment) {
        Order order = orderCache.get(orderId);
        collector.emit(new Values(payment.amount, RaceUtils.millisToSecondsOfMinute(payment.createTime),
                payment.platform, order.platform));
        if (Math.abs(order.amount-payment.amount) < 0.0001) {
            orderCache.remove(orderId);
            paymentCache.remove(orderId);
        } else if (order.amount > payment.amount) {
            order.amount = order.amount - payment.amount;
            orderCache.put(orderId, order);
        } else {
            LOG.error("%%%%%%: order amount :" + order.amount + " less than payment amount: " + payment.amount);
        }
//        join_out_count ++;
    }

    void processOrder(long orderId, Order order) {
        Payment payment = paymentCache.get(orderId);
        while (payment != null) {
            collector.emit(new Values(payment.amount, RaceUtils.millisToSecondsOfMinute(payment.createTime),
                    payment.platform, order.platform));
            order.amount = order.amount - payment.amount;
            payment = payment.next;
        }
        if (Math.abs(order.amount) < 0.0001) {
            paymentCache.remove(orderId);
        } else if (order.amount > 0.0) {
            orderCache.put(orderId, order);
        } else {
            LOG.error("%%%%%% : left order amount :" + order.amount +" less than 0.");
        }
//        join_out_count ++;
    }

    @Override
    public void cleanup() {
//        LOG.info("%%%%%% cleanup. order cache size: " + orderCache.size());
//        LOG.info("%%%%%% cleanup. payment cache size: " + paymentCache.size());
//        LOG.info("%%%%%% payment count:" + pay_count +", order count:: " + order_count);
//        LOG.info("%%%%%% payment amount count:" + pay_amount_count +", order amount count:: " + order_amount_count);
//        LOG.info("%%%%%% join out count:" + join_out_count);
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