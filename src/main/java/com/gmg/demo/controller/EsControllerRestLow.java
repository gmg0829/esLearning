package com.gmg.demo.controller;

/**
 * @author gmg
 * @title: NeoControllerHttp
 * @projectName neo4j_demo
 * @description: TODO
 * @date 2019/4/30 12:48
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gmg.demo.util.EsRestLowUtil;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

@RestController
@RequestMapping("low")
public class EsControllerRestLow {

 @RequestMapping("basic")
 public String basic() throws IOException {
     RestClient restClient= EsRestLowUtil.getRestClient();

     String queryString = "{" +
             "  \"size\": 20," +
             "  \"query\": {" +
             "   \"range\": {" +
             "     \"createTime\": {" +
             "       \"gte\": \"2018-06-01 00:00:00\"" +
             "     }" +
             "   }" +
             "  }" +
             "}";

     HttpEntity entity = new NStringEntity(queryString, ContentType.APPLICATION_JSON);

     try {

         Request request = new Request(
                 "GET",
                 "/some_important_index*/_search");
         request.setEntity(entity);

         Response response = restClient.performRequest(request);
         System.out.println(response.getStatusLine().getStatusCode());
         String responseBody = null;

         responseBody = EntityUtils.toString(response.getEntity());
         System.out.println("******************************************** ");

         JSONObject jsonObject = JSON.parseObject(responseBody);


         System.out.println(jsonObject.get("hits"));
     }catch (Exception e){
         e.printStackTrace();
     }finally {
         restClient.close();
     }

     return "success";
 }

}

