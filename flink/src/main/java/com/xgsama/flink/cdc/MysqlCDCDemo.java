package com.xgsama.flink.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * MysqlCDCDemo
 *
 * @author : xgSama
 * @date : 2022/2/11 16:25:36
 */
public class MysqlCDCDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000L);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String createSql =
                "CREATE TABLE user_source (\n" +
                        "  `id` int,\n" +
                        "  name varchar(30),\n" +
                        "  age int,\n" +
                        "  PRIMARY KEY (`id`) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "  'connector' = 'mysql-cdc',\n" +
                        "  'hostname' = '47.103.218.168',\n" +
                        "  'port' = '3307',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = 'cyz19980815',\n" +
                        "  'database-name' = 'test',\n" +
                        "  'table-name' = 'iceberg_test'\n" +
                        ")";


//                "CREATE TABLE KafkaTable (\n"  +
//                        "  `user_id` BIGINT,\n" +
//                        "  `item 2 id` BIGINT,\n" +
//                        "  `item` ROW(`id 1 di` STRING, `age` BIGINT)\n" +
//                        ") WITH (\n" +
//                        "  'connector' = 'kafka',\n" +
//                        "  'format' = 'json',\n" +
//                        "  'topic' = 'zy-topic-demo',\n" +
//                        "  'properties.bootstrap.servers' = '172.16.100.109:9092',\n" +
//                        "  'properties.group.id' = 'testGroup',\n" +
//                        "  'scan.startup.mode' = 'earliest-offset'," +
//                        "  'json.ignore-parse-errors' = 'true'\n" +
//                        ")";

        tableEnv.executeSql(createSql);

        tableEnv.executeSql("select * from user_source").print();


        env.execute();
    }
}
