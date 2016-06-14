package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {

    //这些是写tair key的前缀
    public static String prex_tmall = "platformTmall_";
    public static String prex_taobao = "platformTaobao_";
    public static String prex_ratio = "ratio_";


    //这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
    public static String JstormTopologyName = "Preliminary";

    public static String MqNamesrvAddr = "172.168.170.79:9876";
    public static String MqConsumerGroup = "RaceConsumerGroup";
    public static String MqProducerGroup = "RaceProducerGroup";
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";

    public static String TairConfigServer = "172.168.170.79:5198";
    public static String TairSalveConfigServer = null;
    public static String TairGroup = "group_1";
    public static Integer TairNamespace = 1;
}
