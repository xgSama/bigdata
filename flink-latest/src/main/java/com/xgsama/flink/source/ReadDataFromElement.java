package com.xgsama.flink.source;

import com.xgsama.flink.model.Student;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ReadDataFromElement
 *
 * @author : xgSama
 * @date : 2022/1/11 22:06:20
 */
public class ReadDataFromElement {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> elementSource = env.fromElements(1, 1, 3, 4, 5);

        DataStreamSource<Student> pojoSource = env.fromElements(Student.class, new Student(1, "c", "123", 1));

        pojoSource.print();

        elementSource.print();

        env.execute();
    }
}
