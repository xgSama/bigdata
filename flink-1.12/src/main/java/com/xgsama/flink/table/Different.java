package com.xgsama.flink.table;

import com.xgsama.flink.func.udf.GenerateUUID;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Different
 *
 * @author: xgsama
 * @date: 2022/4/1 20:58:13
 */
public class Different {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        tableEnv.executeSql(
                "create table table1 ( \n" +
                        "id int, \n" +
                        "class varchar, \n" +
                        "proctime as proctime()) \n" +
                        "with ( \n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'test',\n" +
                        "  'properties.bootstrap.servers' = 'minio:9092',\n" +
                        "  'scan.startup.mode' = 'latest-offset',\n" +
                        "  'format' = 'json'" +
                        ")");

        tableEnv.executeSql(
                "create table uuid1 (\n" +
                        "  id int,\n" +
                        "  uuid varchar,\n" +
                        "  PRIMARY KEY (id) NOT ENFORCED\n" +
                        ") WITH ( \n" +
                        "   'connector' = 'jdbc',\n" +
                        "   'url' = 'jdbc:mysql://172.16.100.139:3306/bigdata?useSSL=false',\n" +
                        "   'table-name' = 'uuid_sink1',\n" +
                        "   'username' = 'root',\n" +
                        "   'password' = 'abc123'\n" +
                        ")");

        tableEnv.executeSql(
                "create table uuid2 (\n" +
                        "  id int,\n" +
                        "  uuid varchar,\n" +
                        "  PRIMARY KEY (id) NOT ENFORCED\n" +
                        ") WITH ( \n" +
                        "   'connector' = 'jdbc',\n" +
                        "   'url' = 'jdbc:mysql://172.16.100.139:3306/bigdata?useSSL=false',\n" +
                        "   'table-name' = 'uuid_sink2',\n" +
                        "   'username' = 'root',\n" +
                        "   'password' = 'abc123'\n" +
                        ")");

        tableEnv.createFunction("getUUID", GenerateUUID.class);
        tableEnv.executeSql("create view mid_view as select id, getUUID() uuid from table1");

        StatementSet statementSet = tableEnv.createStatementSet();
        String sql1 = "insert into uuid1 select id,uuid from mid_view";
        String sql2 = "insert into uuid2 select id,uuid from mid_view";

        statementSet.addInsertSql(sql1);
        statementSet.addInsertSql(sql2);

        statementSet.execute();

        env.execute();

    }
}
