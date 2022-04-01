package com.xgsama.flink.type;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * TypeDemo
 *
 * @author : xgSama
 * @date : 2022/3/31 14:04:51
 */
public class TypeDemo {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .readTextFile("")
                .map(
                        new MapFunction<String, Row>() {
                            @Override
                            public Row map(String value) throws Exception {
                                return null;
                            }
                        }, Types.ROW(Types.INT, Types.STRING));

        Types.TUPLE(Types.INT, Types.STRING);

    }



}
