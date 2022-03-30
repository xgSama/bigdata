package com.xgsama.flink.jdbc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * GenericJdbcSinkFunction
 *
 * @author xgSama
 * @date 2021/4/19 10:36
 */
public class GenericJdbcSinkFunction<T> extends RichSinkFunction<T> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void invoke(T value, Context context) throws Exception {

    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
