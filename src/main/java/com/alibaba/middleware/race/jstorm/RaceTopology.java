package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.middleware.race.PlatformType;
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
        builder.setSpout("payMessage", new PaymentMessageSpout(), 1);
        builder.setSpout("tbMessage", new OrderMessageSpout(PlatformType.Taobao), 1);
        builder.setSpout("tmMessage", new OrderMessageSpout(PlatformType.Tmall), 1);

        builder.setBolt("tbAmount", new AmountCalculateBolt(PlatformType.Taobao), 1)
                .fieldsGrouping("payMessage", new Fields("orderId"))
                .fieldsGrouping("tbMessage", new Fields("orderId"));

        builder.setBolt("tmAmount", new AmountCalculateBolt(PlatformType.Tmall), 1)
                .fieldsGrouping("payMessage", new Fields("orderId"))
                .fieldsGrouping("tmMessage", new Fields("orderId"));

        builder.setBolt("ratio", new RatioCalculateBolt(), 1)
                .shuffleGrouping("payMessage");

        String topologyName = RaceConfig.JstormTopologyName;

        try {
            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}