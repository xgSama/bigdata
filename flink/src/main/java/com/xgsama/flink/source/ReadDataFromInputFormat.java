package com.xgsama.flink.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * ReadDataFromInputFormat
 *
 * @author : xgSama
 * @date : 2022/1/13 17:26:36
 */
public class ReadDataFromInputFormat {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDBUrl("jdbc:mysql://xgsama:3307/test?useSSL=false")
                .setUsername("root")
                .setPassword("cyz19980815")
                .setDrivername("com.mysql.jdbc.Driver")
                .setQuery("select * from student;")
                .setRowTypeInfo((new RowTypeInfo(
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO)))
                .finish();
        DataStreamSource<Row> source = env.createInput(jdbcInputFormat);
        source.setParallelism(2);

        SingleOutputStreamOperator<Row> out = source.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) throws Exception {
                return value;
            }
        }).setParallelism(3);

        out.print();

        env.execute();
    }
}
