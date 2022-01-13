package com.xgsama.flink.function.source;

import com.xgsama.flink.model.Student;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * MySqlFunctionTest
 *
 * @author : xgSama
 * @date : 2022/1/13 10:21:22
 */
public class MySqlParallelFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<Student> studentDataStreamSource = env.addSource(new MysqlParallelSourceFunction());

        studentDataStreamSource.print();


        env.execute();
    }
}
