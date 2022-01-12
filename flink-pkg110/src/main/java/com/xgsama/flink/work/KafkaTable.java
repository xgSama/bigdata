package com.xgsama.flink.work;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

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
        String createSql =
                "CREATE TABLE KafkaTable (\n"  +
                "  `user_id` BIGINT,\n" +
                "  `item 2 id` BIGINT,\n" +
                "  `item` ROW(`id 1 di` STRING, `age` BIGINT)\n" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.version' = 'universal',\n " +
                "  'format.type' = 'json',\n" +
                "  'connector.topic' = 'zy-topic-demo',\n" +
                "  'connector.properties.bootstrap.servers' = '172.16.100.109:9092',\n" +
                "  'connector.properties.group.id' = 'testGroup',\n" +
                "  'connector.startup-mode' = 'earliest-offset' \n" +
                ")";

        tableEnv.sqlUpdate(createSql);

        Table table = tableEnv.sqlQuery("select * from KafkaTable");

        tableEnv.toRetractStream(table, Row.class).print();


        env.execute();
    }
}
