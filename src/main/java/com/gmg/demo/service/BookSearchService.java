package com.gmg.demo.service;

import com.gmg.demo.bean.Book;
import com.gmg.demo.common.Constants;
import com.gmg.demo.common.Response;
import com.gmg.demo.common.ResponsePage;
import com.gmg.demo.form.BoolForm;
import com.gmg.demo.form.MatchForm;
import com.gmg.demo.util.CommonQueryUtils;
import com.gmg.demo.util.EsConfig;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

/**
 * @author gmg
 * @title: BookSearchService
 * @projectName esLearning
 * @description: TODO
 * @date 2019/8/18 10:25
 */
@Service
public class BookSearchService {
     @Autowired
    RestHighLevelClient restHighLevelClient;

     @Autowired
    EsConfig esConfig;


    /**　
     * 1.1 对 "guide" 执行全文检索
     */
    public Response<List<Book>> matchQuery(String query) {

//        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("summary", query);
//        MultiMatchQueryBuilder queryBuilder = new MultiMatchQueryBuilder(query).field("title").field("summary"); //多行
//          3、 Boosting提升某字段得分的检索( Boosting)，将“摘要”字段的得分提高了3倍
        MultiMatchQueryBuilder queryBuilder = new MultiMatchQueryBuilder(query).field("title").field("summary", 3);
        //MultiMatchQueryBuilder queryBuilder = new MultiMatchQueryBuilder(query);
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        ssb.query(queryBuilder);

        //3、构造请求对象查询
        SearchRequest searchRequest = new SearchRequest(esConfig.getBookIndex());
        searchRequest.source(ssb);

        SearchResponse searchResponse = null;
        try {
            searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return CommonQueryUtils.buildResponse(searchResponse);
    }

    /**
     * 1.2 在标题字段(title)中搜索带有 "in action" 字样的图书
     */
    public ResponsePage<List<Book>> match(MatchForm form) {
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("title", form.getTitle());
        // 高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder().field("title").fragmentSize(200);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder
                .query(matchQueryBuilder)
                .from(form.getFrom())
                .size(form.getSize())
                .highlighter(highlightBuilder)
                // 设置 _source 要返回的字段
                .fetchSource(Constants.fetchFieldsTSPD, null);

        //3、构造请求对象查询
        SearchRequest searchRequest = new SearchRequest(esConfig.getBookIndex());
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = null;
        try {
            searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }

        int total = (int)searchResponse.getHits().getTotalHits().value;
        return CommonQueryUtils.buildResponsePage(searchResponse, form.getFrom(), form.getSize(), total);
    }

    /**
     * 4、Bool检索( Bool Query) :
     * 在标题中搜索一本名为 "Elasticsearch" 或 "Solr" 的书，
     * AND由 "clinton gormley" 创作，但NOT由 "radu gheorge" 创作
     */
    public Response<List<Book>> bool(BoolForm form) {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();

        // 搜索标题 should
        BoolQueryBuilder shouldTitleBool = new BoolQueryBuilder();
        form.getShouldTitles().forEach(title -> {
            shouldTitleBool.should().add(new MatchQueryBuilder("title", title));
        });
        boolQuery.must().add(shouldTitleBool);
        // match 作者
        form.getMustAuthors().forEach(author -> {
            boolQuery.must().add(new MatchQueryBuilder("authors", author));
        });
        // not match 作者
        form.getMustNotAuthors().forEach(author -> {
            boolQuery.mustNot().add(new MatchQueryBuilder("authors", author));
        });

        SearchSourceBuilder ssb = new SearchSourceBuilder();
        ssb.query(boolQuery);

        //3、构造请求对象查询
        SearchRequest searchRequest = new SearchRequest(esConfig.getBookIndex());
        searchRequest.source(ssb);

        SearchResponse searchResponse = null;
        try {
            searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return CommonQueryUtils.buildResponse(searchResponse);
    }

    /**
     * 6、 Wildcard Query 通配符检索
     * 要查找具有以 "t" 字母开头的作者的所有记录
     */
    public Response<List<Book>> wildcard(String fieldName, String pattern) {
        WildcardQueryBuilder wildcardQueryBuilder = new WildcardQueryBuilder(fieldName, pattern);
        HighlightBuilder highlightBuilder = new HighlightBuilder().field(Constants.AUTHORS, 200);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

       searchSourceBuilder.query(wildcardQueryBuilder)
                .fetchSource(Constants.fetchFieldsTA, null)
                .highlighter(highlightBuilder);

        //3、构造请求对象查询
        SearchRequest searchRequest = new SearchRequest(esConfig.getBookIndex());
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = null;
        try {
            searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return CommonQueryUtils.buildResponse(searchResponse);
    }



}
