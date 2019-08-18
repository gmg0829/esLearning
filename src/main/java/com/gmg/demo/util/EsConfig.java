package com.gmg.demo.util;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author gmg
 * @title: EsConfig
 * @projectName esLearning
 * @description: TODO
 * @date 2019/8/16 11:12
 */
@Configuration
@ConfigurationProperties(prefix="elasticsearch")
public class EsConfig {

    private String hostlist;

    private  String bookIndex;

    @Bean
    public RestHighLevelClient restHighLevelClient(){
        List<String> list=Arrays.asList(hostlist.split("'"));

        List<HttpHost> httpHostList = new ArrayList(list.size());
        //封装es服务端地址
        for(String host:list){
            HttpHost httpHost = new HttpHost(host.split(":")[0], Integer.parseInt(host.split(":")[1]), "http");
            httpHostList.add(httpHost);
        }
        return new RestHighLevelClient(RestClient.builder(httpHostList.toArray(new HttpHost[0])));
    }

    public String getHostlist() {
        return hostlist;
    }

    public void setHostlist(String hostlist) {
        this.hostlist = hostlist;
    }

    public String getBookIndex() {
        return bookIndex;
    }

    public void setBookIndex(String bookIndex) {
        this.bookIndex = bookIndex;
    }
}
