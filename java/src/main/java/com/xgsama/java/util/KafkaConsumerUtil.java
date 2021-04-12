package com.xgsama.java.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * KafkaConsumerUtil
 *
 * @author xgSama
 * @date 2020/11/5 11:01
 */
public class KafkaConsumerUtil {

    private KafkaConsumer<String, String> consumer = null;
    private Properties props = new Properties();
    private List<String> topics = null;
    private Duration pollSize = Duration.ofMillis(1000);
    private String zkHosts;
    private boolean status = true;

    public KafkaConsumerUtil() {

        // 设置自动提交offset的延时(可能会造成重复消费的情况)
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");

        // key-value的反序列化
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

//        consumer.listTopics()
    }

}
