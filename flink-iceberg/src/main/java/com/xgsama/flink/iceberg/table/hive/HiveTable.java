package com.xgsama.flink.iceberg.table.hive;

import com.xgsama.flink.util.EnvUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Map;


/**
 * HiveTable
 *
 * @author : xgSama
 * @date : 2022/2/15 11:38:04
 */
public class HiveTable {
    public static void main(String[] args) throws Exception {
        EnvUtil.setHadoopUserName("admin");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000L);


        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        // Configuration tableConfiguration = tableEnv.getConfig().getConfiguration();
        // tableConfiguration.setBoolean("table.dynamic-table-options.enabled", true);

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


        String catalogSql =
                EnvUtil.createHiveCatalogSql();


        tableEnv.executeSql(catalogSql);

        String createDataBaseSql = "create database if not exists `hive_catalog`.`iceberg_db`";
        tableEnv.executeSql(createDataBaseSql);

        String createTableSql =
                "CREATE TABLE if not exists `hive_catalog`.`iceberg_db`.`sample` (\n" +
                        "  `id` int,\n" +
                        "  name varchar(30),\n" +
                        "  age int\n" +
                        ")";

        tableEnv.executeSql(createTableSql);

        tableEnv.executeSql("insert into `hive_catalog`.`iceberg_db`.`sample` select * from user_source ");
//        tableEnv.executeSql("select * from `hive_catalog`.`iceberg_db`.`sample` " +
//                "/*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/ ")
//                .print();

    }
}
