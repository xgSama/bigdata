package com.xgsama.flink.source;

import com.xgsama.flink.function.source.MysqlSourceRichFunction;
import com.xgsama.flink.model.Student;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ReadDataFromDataSource
 *
 * @author : xgSama
 * @date : 2022/1/12 17:52:15
 */
public class ReadDataFromDataSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Student> studentDataStreamSource = env.addSource(new MysqlSourceRichFunction());
        studentDataStreamSource.print();

        env.execute();
    }
}
