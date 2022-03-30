package com.xgsama.flink.function.source;

import com.xgsama.flink.model.Student;
import com.xgsama.flink.source.function.MysqlSourceRichFunction;
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
public class MySqlRichFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Student> studentDataStreamSource = env.addSource(new MysqlSourceRichFunction());

        SingleOutputStreamOperator<Student> map = studentDataStreamSource.map(new MapFunction<Student, Student>() {
            @Override
            public Student map(Student value) throws Exception {
                return value;
            }
        });

        KeyedStream<Student, String> studentStringKeyedStream = map.keyBy(new KeySelector<Student, String>() {
            @Override
            public String getKey(Student value) throws Exception {
                return value.getGender();
            }
        });


        studentStringKeyedStream.sum("age").print();


        env.execute();
    }
}
