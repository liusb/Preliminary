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
    protected transient long baseBeginMinute;
    protected transient long baseEndMinute;
    protected transient long updateBeginMinute;
    protected transient long updateEndMinute;
//    private long in_count = 0L;
//    private double in_tm_amount_count = 0.0;
//    private double in_tb_amount_count = 0.0;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        tairClient = new TairOperatorImpl();
        slots = new HashMap<Long, AmountSlot>();
        baseBeginMinute = Long.MAX_VALUE;
        baseEndMinute = Long.MIN_VALUE;
        cacheSlots = new HashMap<Long, AmountSlot>();
        updateBeginMinute = Long.MAX_VALUE;
        updateEndMinute = Long.MIN_VALUE;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            if(!isTickTuple(tuple)) {
                long minute = tuple.getLong(0);
                AmountSlot amountSlot = cacheSlots.get(minute);
                if (amountSlot == null) {
                    amountSlot = new AmountSlot();
                    cacheSlots.put(minute, amountSlot);
                    if (minute > updateEndMinute) {
                        updateEndMinute = minute;
                    }
                    if (minute < updateBeginMinute) {
                        updateBeginMinute = minute;
                    }
                }
                amountSlot.tmAmount += tuple.getDouble(1);
                amountSlot.tbAmount += tuple.getDouble(2);
                amountSlot.pcAmount += tuple.getDouble(3);
                amountSlot.wirelessAmount += tuple.getDouble(4);
//                in_count++;
//                in_tm_amount_count += tuple.getDouble(1);
//                in_tb_amount_count += tuple.getDouble(2);
            } else {
                writeCache();
            }
            // collector.ack(tuple);
        }catch (Exception e) {
            // collector.fail(tuple);
            LOG.error("Bolt execute failed " + e);
        }
    }

    private void fillSlot(long begin, long end) {
        for(long key = begin; key <= end; key+=60) {
            AmountSlot slot = new AmountSlot();
            if (slots.containsKey(key-60)) {
                AmountSlot lastSlot = slots.get(key-60);
                slot.pcAmount = lastSlot.pcAmount;
                slot.wirelessAmount = lastSlot.wirelessAmount;
            }
            slots.put(key, slot);
        }
    }

    private void writeCache() {
        //LOG.info("%%%%%%: Got tick tuple with cache slot size: " + cacheSlots.size());
        if (cacheSlots.isEmpty()) {
            return;
        }
        if(baseBeginMinute <= baseEndMinute) {
            if(updateBeginMinute < baseBeginMinute) {
                fillSlot(updateBeginMinute, baseBeginMinute-60);
                baseBeginMinute = updateBeginMinute;
            }
            if(updateEndMinute > baseEndMinute) {
                fillSlot(baseEndMinute + 60, updateEndMinute);
                baseEndMinute = updateEndMinute;
            }
        }else {
            fillSlot(updateBeginMinute, updateEndMinute);
            baseBeginMinute = updateBeginMinute;
            baseEndMinute = updateEndMinute;
        }
        long pcAmount = 0;
        long wirelessAmount = 0;
        if((updateEndMinute - updateBeginMinute) > 12*60*60) {
            LOG.error("%%%%%%: " + updateBeginMinute + " ==========> " + updateEndMinute + " may have a bug.");
        }
        for (long key = updateBeginMinute; key <= baseEndMinute; key+=60) {
            AmountSlot slot = slots.get(key);
            AmountSlot amountSlot = cacheSlots.get(key);
            if(amountSlot != null) {
                slot.tmAmount += amountSlot.tmAmount;
                slot.tbAmount += amountSlot.tbAmount;
                pcAmount += amountSlot.pcAmount;
                wirelessAmount += amountSlot.wirelessAmount;
                tairClient.write(RaceConfig.prex_tmall + key, AmountSlot.round(slot.tmAmount));
                tairClient.write(RaceConfig.prex_taobao + key, AmountSlot.round(slot.tbAmount));
            }
            slot.pcAmount += pcAmount;
            slot.wirelessAmount += wirelessAmount;
            tairClient.write(RaceConfig.prex_ratio + key, AmountSlot.round(slot.wirelessAmount/slot.pcAmount));
        }
        cacheSlots.clear();
        updateBeginMinute = Long.MAX_VALUE;
        updateEndMinute = Long.MIN_VALUE;
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
//        LOG.info("%%%%%% WriteResult in count:" + in_count);
//        LOG.info("%%%%%% WriteResult tm amount in count:" + in_tm_amount_count);
//        LOG.info("%%%%%% WriteResult tb amount in count:" + in_tb_amount_count);
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
