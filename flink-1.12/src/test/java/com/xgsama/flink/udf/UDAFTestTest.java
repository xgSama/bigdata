package com.xgsama.flink.udf;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * UDAFTestTest
 *
 * @author : xgSama
 * @date : 2021/12/7 15:58:16
 */
public class UDAFTestTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);


        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple5<String, Integer, String, Integer, String>> sourceStream = env
                .readTextFile("input/udaf.txt")
                .map(new MapFunction<String, Tuple5<String, Integer, String, Integer, String>>() {

                    @Override
                    public Tuple5<String, Integer, String, Integer, String> map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Tuple5<>(split[0].trim(), Integer.parseInt(split[1].trim()), split[2], Integer.parseInt(split[3]), split[4]);
                    }
                });


        tableEnv.createTemporaryView("s_table", sourceStream, "name, age, subject, score, gender, pt.proctime");
        tableEnv.registerFunction("subject_count", new UDAFTest());

        String sql = "select name, subject_count(subject) as cnt from s_table group by name";
        Table table = tableEnv.sqlQuery(sql);

        tableEnv.toRetractStream(table, Row.class).print();

        env.execute();

    }
}
