package com.gmg.demo.controller;

import com.gmg.demo.util.EsRestHighUtil;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

/**
 * @author gmg
 * @title: ArangodbController
 * @projectName graphLearning
 * @description: TODO
 * @date 2019/8/9 16:18
 */
@RestController
@RequestMapping("high")
public class ESControllerRestHigh {

    /**
     * 增, source 里对象创建方式可以是JSON字符串，或者Map，或者XContentBuilder 对象
     * @return
     * @throws IOException
     */
    @RequestMapping("indexRequest")
    public String indexRequest() throws IOException {
        RestHighLevelClient highLevelClient= EsRestHighUtil.getRestClient();

        try {
            IndexRequest request = new IndexRequest("posts");
            request.id("1");
            String jsonString = "{" +
                    "\"user\":\"kimchy\"," +
                    "\"postDate\":\"2013-01-30\"," +
                    "\"message\":\"trying out Elasticsearch\"" +
                    "}";
            request.source(jsonString, XContentType.JSON);
            IndexResponse response= highLevelClient.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            highLevelClient.close();
        }

        return "";
    }

    /**
     * 查
     * @return
     * @throws IOException
     */
    @RequestMapping("getRequest")
    public String getRequest() throws IOException {
        RestHighLevelClient highLevelClient= EsRestHighUtil.getRestClient();

        try {
            GetRequest request = new GetRequest("posts", "1","1").version(2);
            GetResponse response= highLevelClient.get(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            highLevelClient.close();
        }

        return "";
    }

    /**
     * 删
     * @return
     * @throws IOException
     */
    @RequestMapping("deleteRequest")
    public String deleteRequest() throws IOException {
        RestHighLevelClient highLevelClient= EsRestHighUtil.getRestClient();

        try {
            DeleteRequest request = new DeleteRequest("posts", "1","1").version(2);
            DeleteResponse deleteResponse = highLevelClient.delete(
                    request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            highLevelClient.close();
        }

        return "";
    }

    /**
     * 修改
     * @return
     * @throws IOException
     */
    @RequestMapping("updateRequest")
    public String updateRequest() throws IOException {
        RestHighLevelClient highLevelClient= EsRestHighUtil.getRestClient();

        try {
            UpdateRequest request = new UpdateRequest("posts", "1","1").version(2);
            UpdateResponse updateResponse = highLevelClient.update(
                    request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            highLevelClient.close();
        }

        return "";
    }


}
