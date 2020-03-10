package com.gmg.demo.util;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * @author gmg
 * @title: EsRestLowUtil
 * @projectName esLearning
 * @description: TODO
 * @date 2019/8/10 15:40
 */
public class EsRestHighUtil {

    public static RestHighLevelClient   getRestClient(){
        RestHighLevelClient restClient = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                ));
        return  restClient;
    }

}
