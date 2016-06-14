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