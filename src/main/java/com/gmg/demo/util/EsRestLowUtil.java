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
                new HttpHost("192.168.254.134", 9200, "http")
        ).build();
        return  restClient;
    }


    public static RestClientBuilder getRestClientBuilder(){
        RestClientBuilder builder = RestClient.builder(new HttpHost("192.168.254.134", 9200, "http"));
        Header[] defaultHeaders = new Header[]{new BasicHeader("header", "value")};
        builder.setDefaultHeaders(defaultHeaders);
        return  builder;
    }




}
