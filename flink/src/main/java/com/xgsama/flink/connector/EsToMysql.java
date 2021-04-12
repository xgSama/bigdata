package com.xgsama.flink.connector;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Map;

/**
 * com.xgsama.flink.connector.EsToMysql
 *
 * @author xgSama
 * @date 2021/1/27 11:06
 */
public class EsToMysql {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Map<String, Object>> source = env.addSource(new RichSourceFunction<Map<String, Object>>() {
            public void run(SourceContext<Map<String, Object>> sourceContext) throws Exception {
            }

            public void cancel() {

            }
        });


    }
}
