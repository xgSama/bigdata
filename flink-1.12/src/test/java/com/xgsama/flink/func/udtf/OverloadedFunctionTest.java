package com.xgsama.flink.func.udtf;

import com.xgsama.flink.util.EnvUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * OverloadedFunctionTest
 *
 * @author: xgsama
 * @date: 2022/3/31 19:51:46
 */
public class OverloadedFunctionTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        String tableName = EnvUtil.crateTable(tableEnv);

        tableEnv.createFunction("overload", OverloadedFunction.class);

        String sql = "select id,c1 from " + tableName + ", lateral table(overload(id,age)) as t(c1)";


        tableEnv.executeSql(sql).print();


        tableEnv.sqlQuery("select id,c1 from " + tableName + ", lateral table(overload()) as t(c1)").printSchema();

    }
}
