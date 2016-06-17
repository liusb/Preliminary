package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RatioCalculateBolt implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(RatioCalculateBolt.class);

    protected OutputCollector collector;
    protected transient TairOperatorImpl tairClient;
    protected transient HashMap<Long, RatioSlot> slots;
    protected transient long endUpdateMinute;

    class RatioSlot {
        RatioSlot() {
            pcAmount = 0;
            wirelessAmount = 0;
        }
        double pcAmount;
        double wirelessAmount;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        tairClient = new TairOperatorImpl();
        slots = new HashMap<Long, RatioSlot>();
        endUpdateMinute = Long.MIN_VALUE;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            MessageTuple messageTuple = (MessageTuple)tuple.getValue(0);
            List<MessageExt> msgList = messageTuple.getMsgList();
            HashMap<Long, RatioSlot> tupleSlots = new HashMap<Long, RatioSlot>();
            long tupleBeginMinute = Long.MAX_VALUE;
            long tupleEndMinute = Long.MIN_VALUE;
            for (MessageExt msg : msgList) {
                byte[] body = msg.getBody();
                if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                    LOG.info("%%%%%%: Got the end signal");
                    continue;
                }
                PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
                long minute = RaceUtils.millisToSecondsOfMinute(paymentMessage.getCreateTime());
                double amount = paymentMessage.getPayAmount();
                short platform = paymentMessage.getPayPlatform();
                RatioSlot minuteSlot;
                if (tupleSlots.containsKey(minute)) {
                    minuteSlot = tupleSlots.get(minute);
                } else {
                    minuteSlot = new RatioSlot();
                    if (minute < tupleBeginMinute) {
                        tupleBeginMinute = minute;
                    }
                    if (minute > tupleEndMinute) {
                        tupleEndMinute = minute;
                    }
                }
                if (platform == 0) {
                    minuteSlot.pcAmount += amount;
                } else {
                    minuteSlot.wirelessAmount += amount;
                }
                tupleSlots.put(minute, minuteSlot);
            }
            if (endUpdateMinute == Long.MIN_VALUE) {
                endUpdateMinute = tupleBeginMinute;
            }else if (endUpdateMinute < tupleBeginMinute && endUpdateMinute+10*60>tupleBeginMinute) {
                RatioSlot minuteSlot = slots.get(endUpdateMinute);
                double ratio = minuteSlot.wirelessAmount/minuteSlot.pcAmount;
                for (long key = endUpdateMinute+60; key <= tupleBeginMinute; key+=60) {
                    tairClient.write(RaceConfig.prex_ratio + key, ratio);
                    slots.put(key, minuteSlot);
                }
            }
            if (endUpdateMinute < tupleEndMinute) {
                endUpdateMinute = tupleEndMinute;
            }
            if (endUpdateMinute-10*60>tupleBeginMinute) {
                tupleBeginMinute = endUpdateMinute;
            }
            RatioSlot ratioSlot = new RatioSlot();
            ratioSlot.pcAmount = 0;
            ratioSlot.wirelessAmount = 0;
            for (long key = tupleBeginMinute; key <= endUpdateMinute; key+=60) {
                if (tupleSlots.containsKey(key)) {
                    RatioSlot temp = tupleSlots.get(key);
                    ratioSlot.pcAmount += temp.pcAmount;
                    ratioSlot.wirelessAmount += temp.wirelessAmount;
                }
                RatioSlot minuteSlot;
                if (slots.containsKey(key)) {
                    minuteSlot = slots.get(key);
                } else if (slots.containsKey(key-60)){
                    minuteSlot = tupleSlots.get(key-60);
                }else {
                    minuteSlot = new RatioSlot();
                }
                minuteSlot.pcAmount += ratioSlot.pcAmount;
                minuteSlot.wirelessAmount += ratioSlot.wirelessAmount;
                double ratio = minuteSlot.wirelessAmount / minuteSlot.pcAmount;
                tairClient.write(RaceConfig.prex_ratio + key, ratio);
                slots.put(key, minuteSlot);
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