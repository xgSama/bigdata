package com.xgsama.flink.work;

import com.xgsama.flink.entity.User;
import com.xgsama.flink.util.MysqlSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Kafka2Mysql
 *
 * @author : xgSama
 * @date : 2021/9/23 10:51:13
 */
public class Kafka2Mysql {

    private static final Logger logger = LoggerFactory.getLogger(Kafka2Mysql.class);

    public static void main(String[] args) throws Exception {

        logger.info("参数测试-----");
        logger.info(Arrays.toString(args));

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        logger.info("参数测试" + parameterTool.get("test"));
        logger.error("参数测试" + parameterTool.get("test"));
        System.out.println(parameterTool.get("test"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.101.50:9092,172.16.104.110:9092,172.16.104.111:9092");

        DataStream<String> source = env
                .addSource(new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<User> userStream = source
                .map((MapFunction<String, User>) s -> {
                    String[] split = s.split(",");

                    return new User(Integer.parseInt(split[0]), split[1], Integer.parseInt(split[2]));
                });

        userStream
                .timeWindowAll(Time.seconds(5L))
                .apply(new AllWindowFunction<User, List<User>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<User> values, Collector<List<User>> out) throws Exception {
                        List<User> persons = Lists.newArrayList(values);

                        if (persons.size() > 0) {
                            System.out.println("5秒的总共收到的条数：" + persons.size());
                            out.collect(persons);
                        }
                    }
                })
                .addSink(new MysqlSink());

        env.execute();
    }
}
