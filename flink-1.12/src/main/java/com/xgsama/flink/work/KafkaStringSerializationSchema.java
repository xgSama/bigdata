package com.xgsama.flink.work;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * KafkaStringSerializationSchema
 *
 * @author : xgSama
 * @date : 2022/1/17 15:00:25
 */
public class KafkaStringSerializationSchema implements KafkaSerializationSchema<String> {

    private final String topic;

    public KafkaStringSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String val, @Nullable Long aLong) {
        System.out.println(aLong);

        return new ProducerRecord<>(topic, val.getBytes(StandardCharsets.UTF_8));
    }

}
