package com.xgsama.flink.iceberg;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * IcebergTableDemo
 *
 * @author : xgSama
 * @date : 2022/2/12 18:10:43
 */
public class IcebergTableDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String sinkSql =
                "CREATE TABLE ds_users_sink (\n" +
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

//        tableEnv.executeSql("insert into ds_users_sink values(1, 'ss', 12)");

        Table table = tableEnv.sqlQuery("select * from ds_users_sink ");


        tableEnv.toAppendStream(table, Row.class).print("sink");

        env.execute();
    }
}
