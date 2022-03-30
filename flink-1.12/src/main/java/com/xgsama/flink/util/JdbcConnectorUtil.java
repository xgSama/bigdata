package com.xgsama.flink.util;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.types.Row;

/**
 * JdbcConnectorUtil
 *
 * @author : xgSama
 * @date : 2021/9/23 11:44:36
 */
public class JdbcConnectorUtil {

    public static DataStreamSource<Row> addSource(StreamExecutionEnvironment env,
                                                  String url, String sql, RowTypeInfo rowTypeInfo,
                                                  String user, String password) {


        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setRowTypeInfo(rowTypeInfo)
                .setQuery(sql)
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl(url)
                .setUsername(user)
                .setPassword(password)
                .finish();

        env.addSource(new InputFormatSourceFunction<>(jdbcInputFormat, TypeExtractor.getInputFormatTypes(jdbcInputFormat)));

        return env.createInput(jdbcInputFormat);

    }

}
