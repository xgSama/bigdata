package com.xgsama.flink.connector;

import com.xgsama.flink.util.GsonUtil;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * EsSinkFunction
 *
 * @author xgSama
 * @date 2021/4/12 13:52
 */
public class EsSinkFunction<T> implements ElasticsearchSinkFunction<T> {
    private final String index;

    public EsSinkFunction(String index) {
        this.index = index;
    }

    public IndexRequest createIndexRequest(T element) {


        return Requests.indexRequest()
                .index(index)
                .source(GsonUtil.toJSONBytes(element), XContentType.JSON);
    }

    @Override
    public void process(T data, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
        requestIndexer.add(createIndexRequest(data));
    }
}
