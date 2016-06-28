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

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        slots = new HashMap<Long, AmountSlot>();
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            if(!isTickTuple(tuple)) {
                AmountSlot amountSlot;
                double amount = tuple.getDouble(0);
                long minute = tuple.getLong(1);
                if (slots.containsKey(minute)) {
                    amountSlot = slots.get(minute);
                } else {
                    amountSlot = new AmountSlot();
                }
                if (tuple.getBoolean(2)) {
                    amountSlot.pcAmount += amount;
                } else {
                    amountSlot.wirelessAmount += amount;
                }
                if (tuple.getBoolean(3)) {
                    amountSlot.tmAmount += amount;
                } else {
                    amountSlot.tbAmount += amount;
                }
                slots.put(minute, amountSlot);
            } else {
                LOG.info("%%%%%%: Got tick tuple with slot size: " + slots.size());
                for (Map.Entry<Long, AmountSlot> slot: slots.entrySet()) {
                    collector.emit(new Values(slot.getKey(), slot.getValue().tmAmount, slot.getValue().tbAmount,
                            slot.getValue().pcAmount, slot.getValue().wirelessAmount));
                }
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
    public void cleanup() { }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("minute", "tmAmount", "tbAmount", "pcAmount", "wirelessAmount"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 3);
        return conf;
    }
}