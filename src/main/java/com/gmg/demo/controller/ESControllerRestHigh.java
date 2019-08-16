package com.gmg.demo.controller;

import com.gmg.demo.util.EsRestHighUtil;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;

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

    @Autowired

    RestHighLevelClient restHighLevelClient;


    /**
     * 新增索引
     * @return
     * @throws IOException
     */
    @RequestMapping("createIndex")
    public boolean createIndex() throws IOException {
        RestHighLevelClient highLevelClient= EsRestHighUtil.getRestClient();
        //创建索引请求对象、并设置索引名称
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("posts");
        //设置参数
        Settings settings = Settings.builder().put("number_of_shards", 1)
                .put("number_of_replicas",0)
                .build();
        createIndexRequest.settings(settings);
        FileInputStream is = new FileInputStream(this.getClass().getResource("/").getPath()+"mapping.json");
        String mappingJson = IOUtils.toString(is);
        //设置映射
        createIndexRequest.mapping("doc",mappingJson,XContentType.JSON);
        //创建索引操作对象
        IndicesClient indices = highLevelClient.indices();
        CreateIndexResponse createIndexResponse = indices.create(createIndexRequest,RequestOptions.DEFAULT);
        //获得相应是否成功
        boolean acknowledged = createIndexResponse.isAcknowledged();
        return acknowledged;
    }

    /**
     * 删除索引
     * @return
     * @throws IOException
     */
    @RequestMapping("deleteIndex")
    public void deleteIndex()throws Exception{
        RestHighLevelClient highLevelClient= EsRestHighUtil.getRestClient();
        //穿建删除索引库请求对象
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("course");
        //删除索引库
        AcknowledgedResponse deleteIndexResponse= highLevelClient.indices().delete(deleteIndexRequest,RequestOptions.DEFAULT);
        //删除结果
        boolean acknowledged = deleteIndexResponse.isAcknowledged();
        System.out.println(acknowledged);
    }


    /**
     * 增, source 里对象创建方式可以是JSON字符串，或者Map，或者XContentBuilder 对象
     * @return
     * @throws IOException
     */
    @RequestMapping("indexRequest")
    public DocWriteResponse.Result indexRequest() throws IOException {
        RestHighLevelClient highLevelClient= EsRestHighUtil.getRestClient();

        InputStream is = new FileInputStream(this.getClass().getResource("/").getPath()+"document.json");
        String docJsonStr = IOUtils.toString(is);
        //获取索引库对象
        //特别注意doc不要掉、否则报错org.elasticsearch.action.ActionRequestValidationException: Validation Failed: 1: type is missing;
        IndexRequest indexRequest = new IndexRequest("posts","doc");
        indexRequest.source(docJsonStr, XContentType.JSON);
        //往索引库添加文档,这个动作也叫索引
        IndexResponse indexResponse = highLevelClient.index(indexRequest,RequestOptions.DEFAULT);
        //打印结果
        return  indexResponse.getResult();
    }

    /**
     * 查
     * @return
     * @throws IOException
     */
    @RequestMapping("getRequest")
    public String getRequest() throws IOException {
            RestHighLevelClient highLevelClient= EsRestHighUtil.getRestClient();


            GetRequest request = new GetRequest("posts","ePBxmGwBbioLHB_bIuDq");
            /*String[] includes = new String[]{"pic","description","studymodel","price","timestamp","price"};
            String[] excludes = Strings.EMPTY_ARRAY;
            FetchSourceContext fetchSourceContext =
                    new FetchSourceContext(true, includes, excludes);
            request.fetchSourceContext(fetchSourceContext);*/

        request.fetchSourceContext(FetchSourceContext.FETCH_SOURCE);

        GetResponse response= highLevelClient.get(request, RequestOptions.DEFAULT);
            if (response.isExists()){
                return  response.getSourceAsString();
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
            DeleteRequest request = new DeleteRequest("posts","1").version(2);
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
    public RestStatus updateRequest() throws IOException {
        RestHighLevelClient highLevelClient= EsRestHighUtil.getRestClient();

        try {
            UpdateRequest request = new UpdateRequest("posts","ePBxmGwBbioLHB_bIuDq");

            /*Map<String, Object> map = new HashMap<String,Object>();
            map.put("name", "Bootstrap框架");
            request.doc(map);*/

            //脚本
            /*Map<String, Object> parameters = singletonMap("count", 4);

            Script inline = new Script(
                    ScriptType.INLINE, "painless",
                    "ctx._source.price += params.count", parameters);
            request.script(inline);*/

            UpdateResponse updateResponse = highLevelClient.update(
                    request, RequestOptions.DEFAULT);
            return  updateResponse.status();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            highLevelClient.close();
        }

        return RestStatus.BAD_GATEWAY;
    }

    @RequestMapping("searchAll")
    public void searchAll()throws Exception{
        RestHighLevelClient highLevelClient= EsRestHighUtil.getRestClient();

        //1、构造sourceBuild(source源)
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(new String[]{"name","description"}, new String[]{})
                .query(QueryBuilders.matchAllQuery()).from(0).size(5);

//        searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
//        searchSourceBuilder.sort(new FieldSortBuilder("price").order(SortOrder.ASC));

        TermsAggregationBuilder aggregation = AggregationBuilders.terms("by_company")
                .field("company.keyword");
        aggregation.subAggregation(AggregationBuilders.avg("average_age")
                .field("price"));
        searchSourceBuilder.aggregation(aggregation);

        //2、构造查询请求对象
        SearchRequest searchRequest = new SearchRequest("posts");
        searchRequest.types("doc")
                .source(searchSourceBuilder);
        //3、client 执行查询
        SearchResponse searchResponse = highLevelClient.search(searchRequest,RequestOptions.DEFAULT);

        //4、打印结果
        SearchHits hits = searchResponse.getHits();
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * term query: 精确查询、在搜索是会精确匹配关键字、搜索关键字不分词
     */
    @RequestMapping("termQuery")
    public void termQuery()throws Exception{

        RestHighLevelClient highLevelClient= EsRestHighUtil.getRestClient();


        //1、设置queryBuilder
        TermQueryBuilder termQueryBuild = QueryBuilders.termQuery("name","Bootstrap框架");

        //2、设置sourceBuilder
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //设置Term query查询
        searchSourceBuilder.query(termQueryBuild)
                .fetchSource(new String[]{"name","studymodel"}, new String[]{});

        //3、构造searchRequest
        SearchRequest searchRequest = new SearchRequest("posts");
        searchRequest
                .source(searchSourceBuilder);
        //4、client发出请求
        SearchResponse searchResponse = highLevelClient.search(searchRequest,RequestOptions.DEFAULT);

        //5、打印结果
        SearchHits hits = searchResponse.getHits();
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    @RequestMapping("testFileter")
    public void testFileter()throws Exception{

        RestHighLevelClient highLevelClient= EsRestHighUtil.getRestClient();

        //构造multiQureyBuilder
       /* MultiMatchQueryBuilder multiQueryBuilder = QueryBuilders.multiMatchQuery("Spring框架","name","description")
                .minimumShouldMatch("50%")//设置百分比
                .field("name", 10);*/

        //构造booleanQueryBuilder
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        //过滤
        boolQueryBuilder.filter(QueryBuilders.termQuery("studymodel", "201001"))
                .filter(QueryBuilders.rangeQuery("price").gte(60).lte(100));

        //2、构造查询源
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        ssb.fetchSource(new String[]{"name","pic"}, new String[]{});
        ssb.query(boolQueryBuilder);

        //3、构造请求对象查询
        SearchRequest searchRequest = new SearchRequest("posts");
        searchRequest.source(ssb);

        //4、client执行查询
        SearchResponse searchResponse = highLevelClient.search(searchRequest,RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }


    @RequestMapping("countQuery")
    public long countQuery() throws Exception{
        CountRequest countRequest = new CountRequest("posts");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        countRequest.source(searchSourceBuilder);
        CountResponse count = restHighLevelClient.count(countRequest,RequestOptions.DEFAULT);
        return count.getCount();

    }

    @RequestMapping("multiSearchRequest")
    public void multiSearchRequest() throws Exception{
        MultiSearchRequest request = new MultiSearchRequest();

        SearchRequest firstSearchRequest = new SearchRequest("posts");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("user", "kimchy"));
        firstSearchRequest.source(searchSourceBuilder);
        request.add(firstSearchRequest);

        SearchRequest secondSearchRequest = new SearchRequest("test");
        searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("user", "luca"));
        secondSearchRequest.source(searchSourceBuilder);
        request.add(secondSearchRequest);

        MultiSearchResponse response = restHighLevelClient.msearch(request, RequestOptions.DEFAULT);
    }


    @RequestMapping("searchTemplateRequest")
    public void searchTemplateRequest() throws Exception{
        SearchTemplateRequest request = new SearchTemplateRequest();
        request.setRequest(new SearchRequest("posts"));

        request.setScriptType(ScriptType.INLINE);
        request.setScript(
                "{" +
                        "  \"query\": { \"match\" : { \"{{field}}\" : \"{{value}}\" } }," +
                        "  \"size\" : \"{{size}}\"" +
                        "}");

        Map<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("field", "title");
        scriptParams.put("value", "elasticsearch");
        scriptParams.put("size", 5);
        request.setScriptParams(scriptParams);

        SearchTemplateResponse response = restHighLevelClient.searchTemplate(request, RequestOptions.DEFAULT);
    }


}
