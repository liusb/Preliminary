package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.middleware.race.RaceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology；
 * 所以这个主类路径一定要正确
 */
public class RaceTopology {

    private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);


    public static void main(String[] args) throws Exception {

        Config conf = new Config();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("message", new MessageSpout(), 1);

        builder.setBolt("messageJoin", new MessageJoinBolt(), 4)
                .fieldsGrouping("message", new Fields("orderId"));

        builder.setBolt("aggregateBolt", new AggregateBolt(), 4)
                .localFirstGrouping("messageJoin");

        builder.setBolt("writeBolt", new WriteResultBolt(), 1)
                .globalGrouping("aggregateBolt");

        try {
            StormSubmitter.submitTopology(RaceConfig.JstormTopologyName, conf, builder.createTopology());
        } catch (Exception e) {
            LOG.error("Failed to submit the topology.");
            e.printStackTrace();
        }
    }
}