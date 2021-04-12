package com.xgsama.flink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Demo
 *
 * @author xgSama
 * @date 2021/1/29 11:12
 */
public class Demo {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.101.50:9092");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        env.addSource(new Source(), "", new TypeInformation<String>() {
            @Override
            public boolean isBasicType() {
                return false;
            }

            @Override
            public boolean isTupleType() {
                return false;
            }

            @Override
            public int getArity() {
                return 0;
            }

            @Override
            public int getTotalFields() {
                return 0;
            }

            @Override
            public Class<String> getTypeClass() {
                return null;
            }

            @Override
            public boolean isKeyType() {
                return false;
            }

            @Override
            public TypeSerializer<String> createSerializer(ExecutionConfig config) {
                return null;
            }

            @Override
            public String toString() {
                return null;
            }

            @Override
            public boolean equals(Object obj) {
                return false;
            }

            @Override
            public int hashCode() {
                return 0;
            }

            @Override
            public boolean canEqual(Object obj) {
                return false;
            }
        });



        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<String>("xingran", new SimpleStringSchema(), consumerProps));
        source.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class Source extends RichSourceFunction<String > {

        @Override
        public void run(SourceContext<String> ctx) throws Exception {

        }

        @Override
        public void cancel() {

        }
    }
}
