package com.xgsama.flink.connector;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * JDBCSink
 *
 * @author xgSama
 * @date 2021/4/12 17:18
 */
public class JDBCSink {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> inputDS = env.readTextFile("input/sensor.txt");


        SingleOutputStreamOperator<ESSink2.Sensor> sourceDS = inputDS
                .map((MapFunction<String, ESSink2.Sensor>) s -> {
                    String[] split = s.split(",");

                    return new ESSink2.Sensor(split[0].trim(), Long.parseLong(split[1].trim()), Double.parseDouble(split[2].trim()));
                });


    }
}
