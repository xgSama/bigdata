package com.xgsama.flink.connector;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.split.JdbcGenericParameterValuesProvider;
import org.apache.flink.connector.jdbc.split.JdbcNumericBetweenParametersProvider;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * ReadMysql
 *
 * @author : xgSama
 * @date : 2022/1/4 16:29:31
 */
public class ReadMysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);


        Serializable[][] serializables = new Serializable[3][];
        serializables[0] = new Long[]{1L, 2L};
        serializables[1] = new Long[]{3L, 5L};
        serializables[2] = new Long[]{6L, 8L};
        JdbcGenericParameterValuesProvider provider = new JdbcGenericParameterValuesProvider(serializables);

        JdbcNumericBetweenParametersProvider ofBatchNum = new JdbcNumericBetweenParametersProvider(1, 8).ofBatchNum(3);

        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDBUrl("jdbc:mysql://rm-bp10661g217i4ze99io.mysql.rds.aliyuncs.com:3306/testforuser?useSSL=false")
                .setUsername("rds_test")
                .setPassword("Testforuser2021")
                .setDrivername("com.mysql.jdbc.Driver")
                .setQuery("select * from zy_sync_flinkx_source where id >= ? and id <= ?;")
                .setRowTypeInfo((new RowTypeInfo(
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO,
                        Types.SQL_TIMESTAMP)))
                .setParametersProvider(provider)
                .finish();


        DataStreamSource<Row> input = env.createInput(jdbcInputFormat);

        input.print();

        env.execute();
    }
}
