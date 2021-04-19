package com.xgsama.flink.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * WordCount
 *
 * @author xgSama
 * @date 2021/4/12 16:11
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> textFile = env.readTextFile("input/word.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = textFile
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] split = value.split(" ");
                        for (String s : split) {
                            out.collect(new Tuple2<>(s, 1));
                        }
                    }
                })
                .keyBy((KeySelector<Tuple2<String, Integer>, Object>) value -> value.f0)
                .sum(1);

        String executionPlan = env.getExecutionPlan();
        System.out.println(executionPlan);
        System.out.println(env);
        result.print();

        env.execute("word-count");
    }
}
