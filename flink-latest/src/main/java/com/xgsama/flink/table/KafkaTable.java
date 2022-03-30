package com.xgsama.flink.table;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * KafkaTable
 *
 * @author : xgSama
 * @date : 2022/1/6 17:49:28
 */
public class KafkaTable {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
//        String createSql =
//                "CREATE TABLE KafkaTable (\n"  +
//                "  `user_id` BIGINT,\n" +
//                "  `item 2 id` BIGINT,\n" +
//                "  `item` ROW(`id 1 di` STRING, `age` BIGINT)\n" +
//                ") WITH (\n" +
//                "  'connector' = 'kafka',\n" +
//                "  'format' = 'json',\n" +
//                "  'topic' = 'zy-topic-demo',\n" +
//                "  'properties.bootstrap.servers' = '172.16.100.109:9092',\n" +
//                "  'properties.group.id' = 'testGroup',\n" +
//                "  'scan.startup.mode' = 'earliest-offset'," +
//                "  'json.ignore-parse-errors' = 'true'\n" +
//                ")";

        String createSql =
                "CREATE TABLE KafkaTable (\n" +
                        "    user_id varchar,\n" +
                        "    click_time TIMESTAMP(3),\n" +
                        "    WATERMARK FOR click_time AS click_time - INTERVAL '4' SECOND\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'test',\n" +
                        "  'properties.bootstrap.servers' = '172.16.101.50:9092,172.16.104.110:9092,172.16.104.111:9092',\n" +
                        "  'properties.group.id' = 'testGroup',\n" +
                        "  'scan.startup.mode' = 'latest-offset',\n" +
                        "  'json.ignore-parse-errors' = 'false',\n" +
                        "  'format' = 'json'\n" +
                        ")";

        tableEnv.executeSql(createSql);

        tableEnv.executeSql("select * from KafkaTable").print();


        env.execute();
    }
}
