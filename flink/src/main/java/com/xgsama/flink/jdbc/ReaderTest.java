package com.xgsama.flink.jdbc;

import com.xgsama.flink.model.Student;
import com.xgsama.flink.util.jdbc.JdbcConnectorUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * ReaderTest
 *
 * @author xgSama
 * @date 2021/4/19 11:45
 */
public class ReaderTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String url = "jdbc:mysql://xgsama:3307/community";
        String user = "root";
        String password = "cyz19980815";


        DataStreamSource<Row> ds = JdbcConnectorUtil.addSource(env, url, "user", user, password, Student.class);


//        env.addSource(new InputFormatSourceFunction<>(finish, TypeExtractor.getInputFormatTypes(finish)));

        ds.print();

        env.execute();

    }

}
