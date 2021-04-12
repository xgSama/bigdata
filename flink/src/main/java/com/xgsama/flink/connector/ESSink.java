package com.xgsama.flink.connector;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * ESSink
 *
 * @author xgSama
 * @date 2021/1/27 11:09
 */
public class ESSink {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputDS = env.readTextFile("input/sensor.txt");
        DataStream<Sensor> sourceDS = inputDS.map((MapFunction<String, Sensor>) s -> {
            String[] split = s.split(",");

            return new Sensor(split[0].trim(), Long.parseLong(split[1].trim()), Double.parseDouble(split[2].trim()));
        });


        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("dtbase", 9200, "http"));

        ElasticsearchSink.Builder<Sensor> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Sensor>() {

                    public IndexRequest createIndexRequest(Sensor element) {
                        Map<String, Object> json = new HashMap<>();

                        json.put("id", element.id);
                        json.put("temperature", element.temperature);
                        json.put("timestamp", element.timestamp);

                        return Requests.indexRequest()
                                .index("my-index")
                                .type("_doc")
                                .source(json);
                    }

                    @Override
                    public void process(Sensor sensor, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        requestIndexer.add(createIndexRequest(sensor));
                    }
                }
        );

        esSinkBuilder.setBulkFlushMaxActions(1);
        esSinkBuilder.setRestClientFactory(clientBuilder -> {

        });

        esSinkBuilder.setBulkFlushMaxActions(1);
        sourceDS.addSink(esSinkBuilder.build());

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
    }

}
