package com.xgsama.flink.source;

import com.xgsama.flink.model.Student;
import com.xgsama.flink.util.SIterator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.LongValueSequenceIterator;

import java.util.Arrays;
import java.util.List;

/**
 * ReadDataFromCollection
 *
 * @author : xgSama
 * @date : 2022/1/11 22:17:59
 */
public class ReadDataFromCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<Integer> dataSource1 = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5));
        dataSource1.print("dataSource1: ");

        List<Student> student = Student.newStudent(3);

        DataStreamSource<Student> dataSource2 =
                env.fromCollection(student, TypeInformation.of(Student.class));
        dataSource2.print("dataSource2: ");

        SIterator<Student> studentSIterator = new SIterator<>();
        studentSIterator.add(Student.randomStudent());

        DataStreamSource<Student> dataSource3 = env.fromCollection(studentSIterator, Student.class);
        dataSource3.print("dataSource3: ");

        DataStreamSource<Student> dataSource4 = env.fromCollection(studentSIterator, TypeInformation.of(Student.class));
        dataSource4.print("dataSource4: ");

        LongValueSequenceIterator lIterator = new LongValueSequenceIterator(1, 10);

        DataStreamSource<LongValue> paralleSource = env.fromParallelCollection(lIterator, LongValue.class);
        paralleSource.print("paralleSource：");

        DataStreamSource<LongValue> paralleSource2 = env.fromParallelCollection(lIterator, TypeInformation.of(LongValue.class));
        paralleSource2.print("paralleSource：");

        env.execute();
    }
}
