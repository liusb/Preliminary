package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.alibaba.middleware.race.RaceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ����һ���ܼ򵥵�����
 * ѡ�ֵ������ύ����Ⱥ���������г�ʱ���õġ�ÿ��ѡ�ֵ����������20���ӣ�һ���������ʱ��
 * ���ǻὫѡ������ɱ����
 */

/**
 * ѡ����������࣬���Ƕ��������com.alibaba.middleware.race.jstorm.RaceTopology
 * ��Ϊ���Ǻ�̨��ѡ�ֵ�git�������ش�����������е������Ĭ����com.alibaba.middleware.race.jstorm.RaceTopology��
 * �����������·��һ��Ҫ��ȷ
 */
public class RaceTopology {

    private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);


    public static void main(String[] args) throws Exception {

        Config conf = new Config();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("payMessage", new MessageSpout(RaceConfig.MqPayTopic), 1);
        builder.setSpout("tbMessage", new MessageSpout(RaceConfig.MqTaobaoTradeTopic), 1);
        builder.setSpout("tmMessage", new MessageSpout(RaceConfig.MqTmallTradeTopic), 1);

        builder.setBolt("tbAmount", new AmountCalculateBolt(RaceConfig.prex_taobao), 1)
                .shuffleGrouping("payMessage").shuffleGrouping("tbMessage");

        builder.setBolt("tmAmount", new AmountCalculateBolt(RaceConfig.prex_tmall), 1)
                .shuffleGrouping("payMessage").shuffleGrouping("tmMessage");

        builder.setBolt("ratio", new RatioCalculateBolt(), 1)
                .shuffleGrouping("payMessage");

        try {
            StormSubmitter.submitTopology(RaceConfig.JstormTopologyName, conf, builder.createTopology());
        } catch (Exception e) {
            LOG.error("Failed to submit the topology.");
            e.printStackTrace();
        }
    }
}