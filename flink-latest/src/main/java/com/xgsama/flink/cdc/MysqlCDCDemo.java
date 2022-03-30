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
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String createKafkaTable =
                "CREATE TABLE user_source (\n" +
                        "  `id` int,\n" +
                        "  name varchar(30),\n" +
                        "  age int\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'format' = 'json',\n" +
                        "  'topic' = 'demo4',\n" +
                        "  'properties.bootstrap.servers' = '172.16.20.21:9092,172.16.20.22:9092',\n" +
                        "  'properties.group.id' = 'testGroup',\n" +
                        "  'scan.startup.mode' = 'latest-offset'," +
                        "  'json.ignore-parse-errors' = 'true'\n" +
                        ")";

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

        tableEnv.executeSql(createKafkaTable);


        String lastValueSql =
                "create view last_user as  select\n" +
                        "    LAST_VALUE(name) as name,\n" +
                        "    id     \n" +
                        "from\n" +
                        "    user_source  \n" +
                        "group by\n" +
                        "    id";

        tableEnv.executeSql(lastValueSql);

        String createMysqlTable =
                "CREATE TABLE user_sink (\n" +
                        "  name STRING,\n" +
                        "  cnt BIGINT,\n" +
                        "  PRIMARY KEY (name) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "   'connector' = 'jdbc',\n" +
                        "   'username' = 'root',\n" +
                        "   'password' = 'abc123',\n" +
                        "   'url' = 'jdbc:mysql://172.16.100.139:3306/bigdata',\n" +
                        "   'table-name' = 'name_cnt'\n" +
                        ")";

        tableEnv.executeSql(createMysqlTable);

        String insertSql = "insert into user_sink select name, count(*) as cnt from last_user group by name";
        tableEnv.executeSql(insertSql).print();


        env.execute();
    }
}
