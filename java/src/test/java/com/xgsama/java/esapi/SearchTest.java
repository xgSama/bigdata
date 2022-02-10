package com.xgsama.java.esapi;

import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * SearchTest
 *
 * @author xgSama
 * @date 2021/1/27 16:58
 */
public class SearchTest {
    RestHighLevelClient client;

    @Before
    public void init() {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("dtbase", 9200, "http")
                )
        );
    }

    @After
    public void stop() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test() {
        SearchRequest searchRequest = new SearchRequest("zy_demo");

        // 构建搜索条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        sourceBuilder.size(100);
        searchRequest.source(sourceBuilder);
        searchRequest.scroll();

        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            System.out.println(searchResponse);
            System.out.println(JSON.toJSONString(searchResponse.getHits()));
            System.out.println("==================");

            for (SearchHit documentFields : searchResponse.getHits().getHits()) {
                System.out.println(documentFields.getSourceAsMap());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @Test
    public void testScroll() throws Exception {
        SearchRequest request = new SearchRequest();
        request.indices("zy_demo");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        // 设置一次检索多少结果
        searchSourceBuilder.size(20);
        request.source(searchSourceBuilder);
        // 设置滚动间隔
        request.scroll(TimeValue.timeValueMinutes(1));
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 读取返回的滚动id 该id指向保持活动状态的搜索上下文，并在后续搜索滚动调用中被需要
        String scrollId = response.getScrollId();
        // 检索第一批结果
        SearchHits hits = response.getHits();
        int i = 2;
        while (hits != null && hits.getHits().length > 0) {
            System.out.println("当前的scrollId: " + scrollId);
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(TimeValue.timeValueSeconds(30));

            SearchResponse scrollResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            // 读取新的滚动id，该id指向保持活动状态的搜索上下文，并在后续搜索滚动调用被需要
            scrollId = scrollResponse.getScrollId();
            // 获取另一批结果
            hits = scrollResponse.getHits();
            System.out.println();
            i++;
        }
        // 清除滚动搜索的上下文
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        // 将滚动id添加进去
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        boolean succeeded = clearScrollResponse.isSucceeded();
        System.out.println("是否清除成功: {}" + succeeded);
    }
}
