package com.xgsama.flink.work;

import com.mysql.jdbc.Driver;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * KafkaToMysql2
 *
 * @author: xgsama
 * @date: 2022/3/19 16:03:48
 */

public class KafkaToMysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "minio:9092");

        env
                .addSource(new FlinkKafkaConsumer<>("comment", new SimpleStringSchema(), kafkaProps))
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        for (String s1 : s.split(",")) {
                            collector.collect(new Tuple2<>(s1, 1));
                        }
                    }
                })
                .keyBy((KeySelector<Tuple2<String, Integer>, String>) value -> value.f0)
                .sum(1)
                .addSink(JdbcSink.sink(
                        "insert into wordcount (word, count) values (?,?) ON DUPLICATE KEY UPDATE count=? ",
                        (JdbcStatementBuilder<Tuple2<String, Integer>>) (preparedStatement, value) -> {
                            preparedStatement.setString(1, value.f0.trim());
                            preparedStatement.setInt(2, value.f1);
                            preparedStatement.setInt(3, value.f1);
                        },
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://minio:3307/test")
                                .withDriverName(Driver.class.getName())
                                .withUsername("root")
                                .withPassword("cyz19980815")
                                .build()
                ));

        env.execute("KafkaToMysql");
    }
}
