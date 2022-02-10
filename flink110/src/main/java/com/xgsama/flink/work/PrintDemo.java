package com.xgsama.flink.work;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * PrintDemo
 *
 * @author : xgSama
 * @date : 2022/1/17 13:38:31
 */
@Slf4j
public class PrintDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String file = "./" + parameterTool.get("file");
        String topic = parameterTool.get("topic");
        String bootstrapServer = parameterTool.get("servers");

        DataStreamSource<String> source = env.readTextFile(file);

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        source.addSink(
                new FlinkKafkaProducer<>(
                        topic,
                        new KafkaStringSerializationSchema(topic),
                        props,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE));

        env.execute();
    }
}
