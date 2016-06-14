package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {

    //��Щ��дtair key��ǰ׺
    public static String prex_tmall = "platformTmall_";
    public static String prex_taobao = "platformTaobao_";
    public static String prex_ratio = "ratio_";


    //��Щjstorm/rocketMq/tair �ļ�Ⱥ������Ϣ����Щ������Ϣ����ʽ�ύ����ǰ�ᱻ����
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
