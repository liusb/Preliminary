package com.alibaba.middleware.race.rocketmq;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class ConsumerFactory {
    private static final Logger	LOG  = LoggerFactory.getLogger(ConsumerFactory.class);

    public static Map<String, DefaultMQPushConsumer> _consumers =
            new HashMap<String, DefaultMQPushConsumer>();

    public static synchronized DefaultMQPushConsumer mkInstance(String topic,
                                                           MessageListenerConcurrently listener)  throws Exception {
        DefaultMQPushConsumer consumer = _consumers.get(topic);
        if (consumer != null) {
            LOG.error("%%%%%%: Consumer Already Started. Topic: " + topic);
            return consumer;
        }

        consumer = new DefaultMQPushConsumer(RaceConfig.MqConsumerGroup);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //consumer.setNamesrvAddr(RaceConfig.MqNamesrvAddr);
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener(listener);

        consumer.start();

        LOG.info("%%%%%%: Consumer Started. Topic: " + topic);
        _consumers.put(topic, consumer);

        return consumer;
    }
}
