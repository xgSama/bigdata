package com.xgsama.flink.connector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Map;

/**
 * com.xgsama.flink.connector.ESSource
 *
 * @author xgSama
 * @date 2021/1/27 11:09
 */
public class ESSource extends RichSourceFunction<Map<String, Object>> {


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    public void run(SourceContext<Map<String, Object>> sourceContext) throws Exception {

    }

    public void cancel() {

    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
