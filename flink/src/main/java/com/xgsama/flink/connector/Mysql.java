package com.xgsama.flink.connector;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * Mysql
 *
 * @author xgSama
 * @date 2021/2/23 17:45
 */
public class Mysql {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Row> input = env.createInput(
                JdbcInputFormat.buildJdbcInputFormat()
                        .setDBUrl("jdbc:mysql://47.103.218.168:3307/test")
                        .setUsername("root")
                        .setPassword("cyz19980815")
                        .setDrivername("com.mysql.jdbc.Driver")
                        .setQuery("select * from dept")
                        .setRowTypeInfo((new RowTypeInfo(
                                BasicTypeInfo.INT_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO)))
                        .finish());

        input.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
