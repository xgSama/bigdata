package com.xgsama.flink.sql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.FormattingMapper;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/**
 * WindowDemo
 *
 * @author : xgSama
 * @date : 2022/2/21 23:15:38
 */
public class WindowDemo {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                // .useBlinkPlanner() Flink 1.14 中移除了旧的规划器，现在只剩Blink了
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        List<Tuple4<String, String, Double, Timestamp>> orders = new ArrayList<>();
        Tuple4<String, String, Double, Timestamp> tuple =
                new Tuple4<>("cyz", "asa", 11.11, Timestamp.valueOf(LocalDateTime.now()));
        orders.add(tuple);

        WatermarkStrategy<Tuple4<String, String, Double, Timestamp>> watermarkStrategy =
                WatermarkStrategy
                        .<Tuple4<String, String, Double, Timestamp>>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                        .withTimestampAssigner(
                                context -> (element, recordTimestamp) -> element.f3.getTime());

        DataStreamSource<Tuple4<String, String, Double, Timestamp>> source = env.fromCollection(orders);


        source.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Tuple4<String, String, Double, Timestamp>>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                        .withTimestampAssigner(watermarkStrategy));

        tableEnv.createTemporaryView(
                "users",
                source
        );


        tableEnv.executeSql("select * from users").print();

    }
}
