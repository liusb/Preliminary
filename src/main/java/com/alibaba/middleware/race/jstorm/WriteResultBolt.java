package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.AmountSlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class WriteResultBolt implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(WriteResultBolt.class);

    protected OutputCollector collector;
    protected transient TairOperatorImpl tairClient;
    protected transient HashMap<Long, AmountSlot> slots;
    protected transient HashMap<Long, AmountSlot> cacheSlots;
    protected transient long lastMinute;
    protected transient long endUpdateMinute;
    protected transient long beginUpdateMinute;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        tairClient = new TairOperatorImpl();
        slots = new HashMap<Long, AmountSlot>();
        lastMinute = Long.MIN_VALUE;
        endUpdateMinute = Long.MIN_VALUE;
        beginUpdateMinute = Long.MAX_VALUE;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            if(!isTickTuple(tuple)) {
                AmountSlot amountSlot;
                long minute = tuple.getLong(0);
                if (cacheSlots.containsKey(minute)) {
                    amountSlot = cacheSlots.get(minute);
                } else {
                    amountSlot = new AmountSlot();
                    if (minute > endUpdateMinute) {
                        endUpdateMinute = minute;
                    }
                    if (minute < beginUpdateMinute) {
                        beginUpdateMinute = minute;
                    }
                }
                amountSlot.tmAmount += tuple.getLong(1);
                amountSlot.tbAmount += tuple.getLong(2);
                amountSlot.pcAmount += tuple.getLong(3);
                amountSlot.wirelessAmount += tuple.getLong(4);
                cacheSlots.put(minute, amountSlot);
            } else {
                writeCache();
            }
            // collector.ack(tuple);
        }catch (Exception e) {
            // collector.fail(tuple);
            LOG.error("Bolt execute failed " + e);
        }
    }

    private void writeCache() {
        LOG.info("%%%%%%: Got tick tuple with cache slot size: " + cacheSlots.size());
        if (cacheSlots.isEmpty()) {
            return;
        }
        if(lastMinute == Long.MIN_VALUE) {
            lastMinute = beginUpdateMinute - 60;
        }else if(beginUpdateMinute > lastMinute) {
            beginUpdateMinute = lastMinute + 60;
        }
        long pcAmount = 0;
        long wirelessAmount = 0;
        for (long key = beginUpdateMinute; key <= endUpdateMinute; key+=60) {
            AmountSlot slot;
            if (slots.containsKey(key)) {
                slot = slots.get(key);
            }else {
                slot = new AmountSlot();
                if (slots.containsKey(key-60)) {
                    AmountSlot lastSlot = slots.get(key - 60);
                    slot.pcAmount = lastSlot.pcAmount;
                    slot.wirelessAmount = lastSlot.wirelessAmount;
                }
            }
            if(cacheSlots.containsKey(key)) {
                AmountSlot cacheSlot = cacheSlots.get(key);
                slot.tmAmount += cacheSlot.tmAmount;
                slot.tbAmount += cacheSlot.tbAmount;
                pcAmount += cacheSlot.pcAmount;
                wirelessAmount += cacheSlot.wirelessAmount;
                tairClient.write(RaceConfig.prex_tmall +key, slot.tmAmount);
                tairClient.write(RaceConfig.prex_taobao + key, slot.tbAmount);
            }
            slot.pcAmount += pcAmount;
            slot.wirelessAmount += wirelessAmount;
            tairClient.write(RaceConfig.prex_ratio + key, slot.wirelessAmount / slot.pcAmount);
            slots.put(key, slot);
        }
        cacheSlots.clear();
        lastMinute = endUpdateMinute;
        endUpdateMinute = Long.MIN_VALUE;
        beginUpdateMinute = Long.MAX_VALUE;
    }

    private boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    @Override
    public void cleanup() {
        writeCache();
        for(Map.Entry entry: slots.entrySet()) {
            LOG.info("writeResult minute: " + entry.getKey() + entry.getValue());
        }
        tairClient.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        return conf;
    }
}