package com.xgsama.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * ValueStateDemo
 *
 * @author : xgSama
 * @date : 2022/2/10 15:55:27
 */
public class ValueStateDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(
                        Tuple2.of(1L, 3L),
                        Tuple2.of(1L, 5L),
                        Tuple2.of(1L, 7L),
                        Tuple2.of(1L, 4L),
                        Tuple2.of(1L, 2L),
                        Tuple2.of(1L, 1L),
                        Tuple2.of(2L, 2L),
                        Tuple2.of(2L, 2L),
                        Tuple2.of(2L, 2L)
                )
                .keyBy(0)
                .flatMap(new CountWindowAverage()).print();

        env.execute();
    }

    static class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {
        private transient ValueState<Tuple2<Long, Double>> sum;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Tuple2<Long, Double>> descriptor =
                    new ValueStateDescriptor<>(
                            "avg",
                            TypeInformation.of(new TypeHint<Tuple2<Long, Double>>() {
                            })
                    );
            sum = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Double>> out) throws Exception {
            if (sum.value() == null) {
                sum.update(Tuple2.of(0L, 0.0));
            }
            Tuple2<Long, Double> currentSum = sum.value();
            currentSum.f0 += 1;
            currentSum.f1 += input.f1;
            sum.update(currentSum);
            // 当累积个数超过两个，进行求合，并把结果发给下游
            if (currentSum.f0 >= 2) {
                out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));

                sum.clear();
            }

        }
    }
}
