package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class RatioCalculateBolt implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(RatioCalculateBolt.class);

    OutputCollector _collector;
    TairOperatorImpl _tairClient;
    HashMap<Long, RatioSlot> _slots;
    long _currentMinute;
    RatioSlot _currentSlot;

    class RatioSlot {
        double pcAmount;
        double wirelessAmount;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        try {
            _collector = collector;
            _tairClient = new TairOperatorImpl();
            _slots = new HashMap<Long, RatioSlot>(20);
            _currentMinute = RaceUtils.millisTosecondsOfMinute(System.currentTimeMillis());
            _currentSlot = new RatioSlot();
            _currentSlot.pcAmount = 0;
            _currentSlot.wirelessAmount = 0;
        }catch (Exception e) {
            LOG.error("Bolt prepare failed " + e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            double amount = tuple.getDouble(1);
            short platform = tuple.getShort(2);
            long minute = tuple.getLong(3);

            if (amount == 0) {
                double ratio = _currentSlot.wirelessAmount / _currentSlot.pcAmount;
                _tairClient.write(RaceConfig.prex_ratio + _currentMinute, ratio);
            } else if (minute == _currentMinute) {
                if (platform == 0) {
                    _currentSlot.pcAmount += amount;
                } else {
                    _currentSlot.wirelessAmount += amount;
                }
            } else if (minute < _currentMinute) {
                for (long i = minute; i < _currentMinute; i += 60) {
                    if (!_slots.containsKey(i)) {
                        //LOG.error("%%%%%%: ratio slots not contain key:" + i);
                        continue;
                    }
                    RatioSlot temp = _slots.get(i);
                    if (platform == 0) {
                        temp.pcAmount += amount;
                    } else {
                        temp.wirelessAmount += amount;
                    }
                    double ratio = temp.wirelessAmount / temp.pcAmount;
                    _tairClient.write(RaceConfig.prex_ratio + i, ratio);
                    _slots.put(i, temp);
                }
                if (platform == 0) {
                    _currentSlot.pcAmount += amount;
                } else {
                    _currentSlot.wirelessAmount += amount;
                }
            } else {
                double ratio = _currentSlot.wirelessAmount / _currentSlot.pcAmount;
                _tairClient.write(RaceConfig.prex_ratio + _currentMinute, ratio);
                _slots.put(_currentMinute, _currentSlot);
                _currentMinute = minute;
                if (platform == 0) {
                    _currentSlot.pcAmount += amount;
                } else {
                    _currentSlot.wirelessAmount += amount;
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