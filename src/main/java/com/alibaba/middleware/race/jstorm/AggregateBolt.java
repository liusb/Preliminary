package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.model.AmountSlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class AggregateBolt implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(AggregateBolt.class);

    protected OutputCollector collector;
    protected transient HashMap<Long, AmountSlot> slots;
    protected transient AmountSlot lastAmountSlot;
    protected transient long lastMinute;
//    private long in_count = 0L;
//    private long out_count = 0L;
//    private double in_amount_count = 0.0;
//    private AmountSlot out_amount_count;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        slots = new HashMap<Long, AmountSlot>();
        lastAmountSlot = null;
        lastMinute = Long.MIN_VALUE;
//        out_amount_count = new AmountSlot();
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            if(!isTickTuple(tuple)) {
                double amount = tuple.getDouble(0);
                long minute = tuple.getLong(1);
                if (lastMinute != minute) {
                    lastMinute = minute;
                    lastAmountSlot = slots.get(minute);
                    if(lastAmountSlot == null) {
                        lastAmountSlot = new AmountSlot();
                        slots.put(minute, lastAmountSlot);
                    }
                }
                if (tuple.getBoolean(2)) {
                    lastAmountSlot.pcAmount += amount;
                } else {
                    lastAmountSlot.wirelessAmount += amount;
                }
                if (tuple.getBoolean(3)) {
                    lastAmountSlot.tmAmount += amount;
                } else {
                    lastAmountSlot.tbAmount += amount;
                }
//                in_count++;
//                in_amount_count += amount;
            } else {
                // LOG.info("%%%%%%: Got tick tuple with slot size: " + slots.size());
                for (Map.Entry<Long, AmountSlot> slot: slots.entrySet()) {
                    AmountSlot value = slot.getValue();
                    collector.emit(new Values(slot.getKey(), value.tmAmount, value.tbAmount,
                            value.pcAmount, value.wirelessAmount));
//                    out_count++;
//                    out_amount_count.tmAmount += slot.getValue().tmAmount;
//                    out_amount_count.tbAmount += slot.getValue().tbAmount;
//                    out_amount_count.pcAmount += slot.getValue().pcAmount;
//                    out_amount_count.wirelessAmount += slot.getValue().wirelessAmount;
                }
                lastAmountSlot = null;
                lastMinute = Long.MIN_VALUE;
                slots.clear();
            }
        }catch (Exception e) {
            LOG.error("Bolt execute failed " + e);
        }
    }

    private boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    @Override
    public void cleanup() {
//        LOG.info("%%%%%% Aggregate in count:" + in_count +", out count:: " + out_count);
//        LOG.info("%%%%%% Aggregate amount in count:" + in_amount_count);
//        LOG.info("%%%%%% Aggregate amount out count:" + out_amount_count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("minute", "tmAmount", "tbAmount", "pcAmount", "wirelessAmount"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        return conf;
    }
}