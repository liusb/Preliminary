package com.alibaba.middleware.race.jstorm;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.PaymentMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaymentMessageSpout extends MessageSpout {
    private static final Logger LOG = LoggerFactory.getLogger(PaymentMessageSpout.class);

    public PaymentMessageSpout(String topic) {
        super(topic);
    }

    public void putMessage(byte[] body) {
        PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
        while (true) {
            try {
                sendingQueue.put(new Values(paymentMessage.getOrderId(), paymentMessage.getPayAmount(),
                        paymentMessage.getPayPlatform() == 0,
                        RaceUtils.millisToSecondsOfMinute(paymentMessage.getCreateTime())));
                break;
            } catch (Exception e) {
                LOG.info("Failed to blocking the putMessage.");
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("orderId", "payAmount", "payPlatform", "minute"));
    }
}
