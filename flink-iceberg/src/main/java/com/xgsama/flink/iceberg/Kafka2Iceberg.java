package com.xgsama.flink.iceberg;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Kafka2Iceberg
 *
 * @author : xgSama
 * @date : 2022/2/11 18:00:51
 */
public class Kafka2Iceberg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(10 * 1000L);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String createSql =
                "CREATE TABLE user_source (\n" +
                        "  `id` int,\n" +
                        "  name varchar(30),\n" +
                        "  age int\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'format' = 'json',\n" +
                        "  'topic' = 'test8',\n" +
                        "  'properties.bootstrap.servers' = '172.16.20.21:9092,172.16.20.22:9092',\n" +
                        "  'properties.group.id' = 'testGroup',\n" +
                        "  'scan.startup.mode' = 'latest-offset'," +
                        "  'json.ignore-parse-errors' = 'true'\n" +
                        ")";

        tableEnv.executeSql(createSql);

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

        tableEnv.executeSql("insert into all_users_sink select * from user_source");

    }
}
