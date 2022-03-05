package com.xgsama.flink.work;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.Collections;

/**
 * ParamTest
 *
 * @author : xgSama
 * @date : 2021/10/13 17:17:23
 */
@Slf4j
public class ParamTest {

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        log.info("input args{}", Arrays.toString(args));

        String test1 = parameterTool.get("test");

        for (int i = 0; i < 100; i++) {
            log.info("flink -- flink -- flink -----------");
            log.warn("flink -- flink -- flink -----------");
            log.error("flink -- flink -- flink -----------");
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> test = env
                .fromCollection(Collections.singletonList(test1));

        test.print();

        System.out.println(env.getParallelism());

        env.execute();
    }
}
