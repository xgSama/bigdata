package com.xgsama.flink.work;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

/**
 * ParamTest
 *
 * @author : xgSama
 * @date : 2021/10/13 17:17:23
 */
public class ParamTest {

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String test1 = parameterTool.get("test");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> test = env
                .fromCollection(Collections.singletonList(test1));

        test.print();

        System.out.println(env.getParallelism());

        env.execute();
    }
}
