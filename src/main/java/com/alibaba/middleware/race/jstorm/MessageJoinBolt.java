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
import java.util.Map;


public class MessageJoinBolt implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(MessageJoinBolt.class);

    protected OutputCollector collector;
    protected transient HashMap<Long, Boolean> orderCache;
    protected transient HashMap<Long, Payment> paymentCache;
//    private long pay_count = 0L;
//    private long order_count = 0L;
//    private double pay_amount_count = 0.0;
//    private double order_amount_count = 0.0;
//    private long join_out_count = 0L;

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
        orderCache = new HashMap<Long, Boolean>(512);
        paymentCache = new HashMap<Long, Payment>(256);
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
                    processPayment(orderId, payment);
                }
            } else {
//                order_count = order_count+1;
//                order_amount_count += tuple.getDouble(1);
                if(!orderCache.containsKey(orderId)) {
                    Boolean platform = tuple.getBoolean(2);
                    orderCache.put(orderId, platform);
                    processOrder(orderId, platform);
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
        Boolean platform = orderCache.get(orderId);
        if(platform != null) {
            collector.emit(new Values(payment.amount, RaceUtils.millisToSecondsOfMinute(payment.createTime),
                    payment.platform, platform));
        }
//        join_out_count ++;
    }

    void processOrder(long orderId, Boolean platform) {
        Payment payment = paymentCache.get(orderId);
        while (payment != null) {
            collector.emit(new Values(payment.amount, RaceUtils.millisToSecondsOfMinute(payment.createTime),
                    payment.platform, platform));
            payment = payment.next;
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