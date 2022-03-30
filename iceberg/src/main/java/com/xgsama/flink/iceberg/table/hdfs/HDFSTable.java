package com.xgsama.flink.iceberg.table.hdfs;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * HDFSTable
 *
 * @author : xgSama
 * @date : 2022/2/15 10:23:48
 */
public class HDFSTable {
    public static void main(String[] args) throws Exception {
        System.getProperties().put("HADOOP_USER_NAME", "root");
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
                        "  'warehouse'='hdfs://server01:9000/hive/iceberg',\n" +
                        "  'format-version'='2'\n" +
                        ")";
        tableEnv.executeSql(sinkSql);

//        tableEnv.executeSql("insert into all_users_sink select * from user_source");

        Table table = tableEnv.sqlQuery("select * from all_users_sink");
        tableEnv.toRetractStream(table, Row.class).print();

        env.execute();
    }
}
