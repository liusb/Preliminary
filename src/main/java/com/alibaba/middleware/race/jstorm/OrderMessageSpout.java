package com.alibaba.middleware.race.jstorm;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderMessageSpout extends MessageSpout {
    private static final Logger LOG = LoggerFactory.getLogger(OrderMessageSpout.class);
    protected boolean platform;

    public OrderMessageSpout(String topic) {
        super(topic);
        this.platform = topic.equals(RaceConfig.MqTmallTradeTopic);
    }

    public void putMessage(byte[] body) {
        OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
        while (true) {
            try {
                sendingQueue.put(new Values(orderMessage.getOrderId(), orderMessage.getTotalPrice(), platform));
                break;
            } catch (Exception e) {
                LOG.info("Failed to blocking the putMessage.");
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("orderId", "totalPrice", "payPlatform"));
    }
}
