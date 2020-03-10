package com.gmg.demo.util;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

/**
 * @author gmg
 * @title: EsRestLowUtil
 * @projectName esLearning
 * @description: TODO
 * @date 2019/8/10 15:40
 */
public class EsRestLowUtil {
    public static RestClient getRestClient(){
        RestClient restClient = RestClient.builder(
                new HttpHost("localhost", 9200, "http")
        ).build();
        return  restClient;
    }

}
