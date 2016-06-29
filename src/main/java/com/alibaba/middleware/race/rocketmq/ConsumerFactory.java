package com.alibaba.middleware.race.rocketmq;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConsumerFactory {
    private static final Logger	LOG  = LoggerFactory.getLogger(ConsumerFactory.class);

    public static DefaultMQPushConsumer consumer = null;

    public static synchronized DefaultMQPushConsumer mkInstance(MessageListenerConcurrently listener)  throws Exception {
        if (consumer != null) {
            LOG.error("%%%%%%: Consumer Already Started.");
            return consumer;
        }

        consumer = new DefaultMQPushConsumer(RaceConfig.MqConsumerGroup);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        // consumer.setNamesrvAddr(RaceConfig.MqNamesrvAddr);

        consumer.subscribe(RaceConfig.MqPayTopic, "*");
        consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
        consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
        consumer.registerMessageListener(listener);

        consumer.start();

        LOG.info("%%%%%%: Consumer Started.");

        return consumer;
    }
}
