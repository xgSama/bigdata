package com.xgsama.flink.iceberg;

import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * WriteIcebergTableDemo
 *
 * @author : xgSama
 * @date : 2022/2/12 18:12:19
 */
public class WriteIcebergTableDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(1000000L);
        env.setStateBackend(new MemoryStateBackend());

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        String sinkSql =
                "CREATE TABLE all_users_sink (\n" +
                        "  `id` int,\n" +
                        "  name varchar(30),\n" +
                        "  age int,\n" +
                        "  PRIMARY KEY (`id`) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "  'connector'='iceberg',\n" +
                        "  'catalog-name'='iceberg_catalog',\n" +
                        "  'catalog-type'='hadoop',  \n" +
                        "  'warehouse'='file:///tmp/iceberg/warehouse',\n" +
                        "  'format-version'='2'\n" +
                        ")";
        tableEnv.executeSql(sinkSql);

        tableEnv.executeSql("insert into all_users_sink(`id`,name,age) values (1, 'sss', 199)");


        env.execute();
    }
}
