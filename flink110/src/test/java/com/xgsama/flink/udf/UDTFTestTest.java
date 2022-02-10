package com.xgsama.flink.udf;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * UDTFTestTest
 *
 * @author : xgSama
 * @date : 2021/12/7 17:14:02
 */
public class UDTFTestTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);


        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);

        DataStreamSource<String> sourceStream = env
                .readTextFile("input/udtf.txt");


        tableEnv.registerFunction("sp", new UDTFTest());

        tableEnv.createTemporaryView("s_table", sourceStream, "s_id");

        String sql = "select s_id,c1,c2 from  s_table ,  lateral table(sp(s_id)) as t(c1,c2)";
        Table table = tableEnv.sqlQuery(sql);

        tableEnv.toRetractStream(table, Row.class).print();

        env.execute();
    }
}
