package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.jstorm.utils.TimeCacheMap;
import com.alibaba.middleware.race.PlatformType;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class AmountCalculateBolt implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(AmountCalculateBolt.class);

    OutputCollector _collector;
    TairOperatorImpl _tairClient;
    String _tairPrefix;
    TimeCacheMap<Long, Double> _orderCache;
    TimeCacheMap<Long, Payment> _paymentCache;
    HashMap<Long, Double> _slots;
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

    public AmountCalculateBolt(PlatformType platform) {
        if (platform == PlatformType.Tmall) {
            _tairPrefix = RaceConfig.prex_tmall;
        }else {
            _tairPrefix = RaceConfig.prex_taobao;
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        try {
            _collector = collector;
            _tairClient = new TairOperatorImpl();
            _orderCache = new TimeCacheMap<Long, Double>(10 * 60);
            _paymentCache = new TimeCacheMap<Long, Payment>(60);
            _slots = new HashMap<Long, Double>(20);
            _currentAmount = 0;
            _currentMinute = RaceUtils.millisTosecondsOfMinute(System.currentTimeMillis());
        }catch (Exception e) {
            LOG.error("Bolt prepare failed " + e);
        }
    }

    private void processPayment(Payment payment) {
        if (payment.createTime == _currentMinute) {
            _currentAmount += payment.amount;
        }else if (payment.createTime > _currentMinute) {
            _tairClient.write(_tairPrefix + _currentMinute, _currentAmount);
            _slots.put(_currentMinute, _currentAmount);
            _currentMinute = payment.createTime;
            _currentAmount = payment.amount;
        }else {
            double amount = payment.amount;
            if (_slots.containsKey(payment.createTime)) {
                amount += _slots.get(payment.createTime);
            }
            _tairClient.write(_tairPrefix + payment.createTime, amount);
            _slots.put(payment.createTime, amount);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String source = tuple.getSourceComponent();
            if (source.startsWith("pay")) {
                long orderId = tuple.getLong(0);
                Payment payment = new Payment(tuple.getDouble(1), tuple.getLong(3));
                if (_orderCache.containsKey(orderId)) {
                    processPayment(payment);
                    double orderAmount = _orderCache.get(orderId);
                    if (orderAmount == payment.amount) {
                        _orderCache.remove(orderId);
                    } else if (orderAmount > payment.amount) {
                        _orderCache.put(orderId, orderAmount - payment.amount);
                    } else {
                        LOG.error("%%%%%%: order amount not equal to payment amount, order_id: " + orderId);
                    }
                } else {
                    _paymentCache.put(orderId, payment);
                }
            } else {
                long orderId = tuple.getLong(0);
                double totalPrice = tuple.getDouble(1);
                if (_paymentCache.containsKey(orderId)) {
                    Payment payment = _paymentCache.get(orderId);
                    processPayment(payment);
                    _paymentCache.remove(orderId);
                } else {
                    _orderCache.put(orderId, totalPrice);
                }
            }
        }catch (Exception e) {
            LOG.error("Bolt execute failed " + e);
        }
    }

    @Override
    public void cleanup() {
       _tairClient.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}