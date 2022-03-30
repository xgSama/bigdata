package com.xgsama.flink.util;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * KafkaTableUtil
 *
 * @author : xgSama
 * @date : 2022/3/3 11:47:10
 */
public class KafkaTableUtil {

    private static final String brokers = "172.16.20.21:9092,172.16.20.22:9092";

    public static void createTable(StreamTableEnvironment tableEnv, boolean isEarlist) {

        String scanMode = isEarlist ? "earliest-offset" : "latest-offset";

        String createSql =
                "CREATE TABLE KafkaTable (\n" +
                        "    user_id varchar,\n" +
                        "    click_time TIMESTAMP(3),\n" +
                        "    WATERMARK FOR click_time AS click_time - INTERVAL '4' SECOND\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'test',\n" +
                        "  'properties.bootstrap.servers' = '" + brokers + "',\n" +
                        "  'properties.group.id' = 'testGroup',\n" +
                        "  'scan.startup.mode' = '" + scanMode + "',\n" +
                        "  'json.ignore-parse-errors' = 'false',\n" +
                        "  'format' = 'json'\n" +
                        ")";

        tableEnv.executeSql(createSql);
    }
}
