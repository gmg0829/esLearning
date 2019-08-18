package com.gmg.demo.controller;

import com.gmg.demo.bean.Book;
import com.gmg.demo.common.Constants;
import com.gmg.demo.common.Response;
import com.gmg.demo.common.ResponsePage;
import com.gmg.demo.form.BoolForm;
import com.gmg.demo.form.MatchForm;
import com.gmg.demo.service.BookSearchService;
import com.gmg.demo.util.DataUtil;
import com.gmg.demo.util.EsConfig;
import com.gmg.demo.util.EsRestHighUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author gmg
 * @title: BookController
 * @projectName esLearning
 * @description: TODO
 * @date 2019/8/18 10:07
 */
@RequestMapping("book")
@RestController
public class BookController {

    @Autowired
    EsConfig esConfig;

    @Autowired
    RestHighLevelClient restHighLevelClient;

    @Autowired
    BookSearchService bookSearchService;

    private Gson gson = new GsonBuilder().setDateFormat("YYYY-MM-dd").create();


    @RequestMapping("createIndex")
    public boolean createIndex() throws IOException {
        //创建索引请求对象、并设置索引名称
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(esConfig.getBookIndex());
        //设置参数
        Settings settings = Settings.builder().put("number_of_shards", 1)
                .put("number_of_replicas",0)
                .build();
        createIndexRequest.settings(settings);
        //创建索引操作对象
        IndicesClient indices = restHighLevelClient.indices();
        CreateIndexResponse createIndexResponse = indices.create(createIndexRequest, RequestOptions.DEFAULT);
        //获得相应是否成功
        boolean acknowledged = createIndexResponse.isAcknowledged();
        return acknowledged;
    }

    @RequestMapping("buckDoc")
    public int buckDoc() throws Exception{
        List<Book> list = DataUtil.batchData();

        BulkRequest request = new BulkRequest();


        // 添加index操作到 bulk 中
        list.forEach(book -> {
            request.add(new IndexRequest(esConfig.getBookIndex()).id(book.getId())
            .source(gson.fromJson(gson.toJson(book), Map.class)));
        });

        BulkResponse bulkReponse=restHighLevelClient.bulk(request,RequestOptions.DEFAULT);
        return bulkReponse.status().getStatus();
    }

    /**
     * 1.1 对 "guide" 执行全文检索
     * 测试：http://localhost:8002/book/matchQuery?query=guide
     */
    @RequestMapping("matchQuery")
    public Response<List<Book>> multiMatch(@RequestParam(value = "query", required = true) String query) {
        return bookSearchService.matchQuery(query);
    }

    /**
     * 1.2 指定特定字段检索
     * 测试：http://localhost:8002/book/match?title=in action&from=0&size=4
     */
    @RequestMapping("match")
    public ResponsePage<List<Book>> match(MatchForm form) {
        return bookSearchService.match(form);
    }

    /**
     * 4、Bool检索( Bool Query)
     * 测试：http://localhost:8002/book/bool?shouldTitles=Elasticsearch&shouldTitles=Solr&mustAuthors=clinton gormely&mustNotAuthors=radu gheorge
     */
    @RequestMapping("bool")
    public Response<List<Book>> bool(@ModelAttribute BoolForm form) {
        return bookSearchService.bool(form);
    }

    /**
     * 5、 Wildcard Query 通配符检索
     * 测试：http://localhost:8002/book/wildcard?pattern=t*
     */
    @RequestMapping("wildcard")
    public Response<List<Book>> wildcard(String pattern) {
        return bookSearchService.wildcard(Constants.AUTHORS, pattern);
    }

}
