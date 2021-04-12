package com.xgsama.java.esapi;

import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * Demo
 *
 * @author xgSama
 * @date 2021/1/27 15:46
 */
public class Demo {

    public static void main(String[] args) {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("dtbase", 9200, "http")
                )
        );

        GetRequest getRequest = new GetRequest("blog", "2");

        try {
            GetResponse documentFields = client.get(getRequest, RequestOptions.DEFAULT);

            System.out.println(documentFields);
            System.out.println(documentFields.getSource());

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }

}
