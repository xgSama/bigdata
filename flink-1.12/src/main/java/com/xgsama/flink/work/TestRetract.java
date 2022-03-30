package com.xgsama.flink.work;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * TestRetract
 *
 * @author : xgSama
 * @date : 2022/1/19 16:27:26
 */
public class TestRetract {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String servers = "172.16.101.50:9092,172.16.104.110:9092,172.16.104.111:9092";

        String createSql =
                "CREATE TABLE retract (\n" +
                        "  `id` BIGINT,\n" +
                        "  `name` VARCHAR,\n" +
                        "  `age` BIGINT,\n" +
                        "  `course` VARCHAR,\n" +
                        "  `times` BIGINT,\n" +
                        "  `score` BIGINT\n" +
                        ") WITH (\n" +
                        "  'connector.type' = 'kafka',\n" +
                        "  'connector.version' = 'universal',\n " +
                        "  'format.type' = 'json',\n" +
                        "  'connector.topic' = 'zy_table_1',\n" +
                        "  'connector.properties.bootstrap.servers' = '" + servers + "',\n" +
                        "  'connector.properties.group.id' = 'testGroup',\n" +
                        "  'connector.startup-mode' = 'latest-offset' \n" +
                        ")";

        tableEnv.sqlUpdate(createSql);

        String g1Sql = "select id, name,count(*) as times from retract group by id, name";
        Table table1 = tableEnv.sqlQuery(g1Sql);
        tableEnv.createTemporaryView("t1", table1);

        String g2Sql = "select id, course,sum(score)  as total from retract group by id, course";
        Table table2 = tableEnv.sqlQuery(g2Sql);
        tableEnv.createTemporaryView("t2", table2);

        String resSql = "select t1.id, name, count(*) as c,sum(total) as total from t1 join t2 on t1.id=t2.id group by t1.id, t1.name";
        Table table3 = tableEnv.sqlQuery(resSql);
        tableEnv.createTemporaryView("res", table3);


        tableEnv.sqlUpdate("CREATE TABLE sink (\n" +
                "    id BIGINT,\n" +
                "    name VARCHAR,\n" +
                "    c BIGINT,\n" +
                "    total  BIGINT\n" +
                ") WITH (\n" +
                "    'connector.type' = 'jdbc',\n" +
                "    'connector.url' = 'jdbc:mysql://47.103.218.168:3307/test?useSSL=false',\n" +
                "    'connector.table' = 'retractSink',\n" +
                "    'connector.username' = 'root',\n" +
                "    'connector.password' = 'cyz19980815',\n" +
                "    'connector.write.flush.max-rows' = '1'\n" +
                ")");

        tableEnv.sqlUpdate("insert into sink select * from res");


        tableEnv.toRetractStream(table3, Row.class).print();

        env.execute();

    }
}
