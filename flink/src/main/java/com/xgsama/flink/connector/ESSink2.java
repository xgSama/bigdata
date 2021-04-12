package com.xgsama.flink.connector;

import com.xgsama.flink.util.EsConnectorUtil;
import com.xgsama.flink.util.GsonUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ESSink
 *
 * @author xgSama
 * @date 2021/1/27 11:09
 */
public class ESSink2 {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputDS = env.readTextFile("input/sensor.txt");


        SingleOutputStreamOperator<Sensor> sourceDS = inputDS.map((MapFunction<String, Sensor>) s -> {
            String[] split = s.split(",");

            return new Sensor(split[0].trim(), Long.parseLong(split[1].trim()), Double.parseDouble(split[2].trim()));
        });

        sourceDS.print("source");

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("dtbase", 9200, "http"));

        EsConnectorUtil.addSink(sourceDS,
                httpHosts,
                10,
                1,
                new EsSinkFunction<>("my-index"));


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class Sensor {
        String id;
        long timestamp;
        double temperature;

        public Sensor(String id, long timestamp, double temperature) {
            this.id = id;
            this.timestamp = timestamp;
            this.temperature = temperature;
        }

        @Override
        public String toString() {
            return "Sensor{" +
                    "id='" + id + '\'' +
                    ", timestamp=" + timestamp +
                    ", temperature=" + temperature +
                    '}';
        }
    }

}
