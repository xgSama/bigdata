package com.xgsama.flink.input;

import com.xgsama.flink.model.SourceJobConf;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * InputTest
 *
 * @author : xgSama
 * @date : 2022/1/10 16:44:41
 */
public class InputTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);


        SourceJobConf.SourceJobConfBuilder builder = SourceJobConf.builder();
        SourceJobConf conf = builder
                .dbUrl("jdbc:mysql://rm-bp10661g217i4ze99io.mysql.rds.aliyuncs.com:3306/testforuser?useSSL=false")
                .user("rds_test")
                .password("Testforuser2021")
                .driver("com.mysql.jdbc.Driver")
                .splitKey("id")
                .build();

        MyJdbcInputFormat inputFormat = new MyJdbcInputFormat(conf);

        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO,
                Types.SQL_TIMESTAMP);

        DataStreamSource<Row> input = env.createInput(inputFormat, rowTypeInfo);


        input.print();
        env.execute();
    }
}
