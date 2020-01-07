package com.gmg.demo.controller;

import com.gmg.demo.util.EsRestHighUtil;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
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
import org.elasticsearch.client.indices.AnalyzeRequest;
import org.elasticsearch.client.indices.AnalyzeResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        //2、构造查询请求对象
        SearchRequest searchRequest = new SearchRequest("posts");
        searchRequest
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

    @RequestMapping("buckRequest")
    public void buckRequest() throws Exception{
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest("posts").id("1")
                .source(XContentType.JSON,"field", "foo"));
        request.add(new IndexRequest("posts").id("2")
                .source(XContentType.JSON,"field", "bar"));
        request.add(new IndexRequest("posts").id("3")
                .source(XContentType.JSON,"field", "baz"));

        BulkResponse bulkReponse=restHighLevelClient.bulk(request,RequestOptions.DEFAULT);
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


    public  void queryBuilders(){
        QueryBuilders.matchQuery("name", "葫芦4032娃");
        QueryBuilders.multiMatchQuery(
                "山西省太原市7429街道",
                "home", "now_home"
        );
        QueryBuilders
                .boolQuery()
                .must(QueryBuilders.termQuery("name", "葫芦3033娃"))
                .must(QueryBuilders.termQuery("home", "山西省太原市7967街道"))
                .mustNot(QueryBuilders.termQuery("isRealMen", false))
                .should(QueryBuilders.termQuery("now_home", "山西省太原市"));

        QueryBuilders.idsQuery().addIds("1111","2222");

        //模糊查询
        QueryBuilders.fuzzyQuery("name", "葫芦3582");

    }

    @RequestMapping("searchAllGroup")
    public void searchAllGroup()throws Exception{
        RestHighLevelClient highLevelClient= EsRestHighUtil.getRestClient();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //
        /*TermsAggregationBuilder aggregation = AggregationBuilders.terms("name_count")
                .field("name");

        aggregation.subAggregation(AggregationBuilders.avg("price_avg")
                .field("price"));

        aggregation.subAggregation(AggregationBuilders.sum("price_sum")
                .field("price"));

        aggregation.subAggregation(AggregationBuilders.min("price_min")
                .field("price"));

        aggregation.subAggregation(AggregationBuilders.max("price_max")
                .field("price"));


        searchSourceBuilder.aggregation(aggregation);*/

        StatsAggregationBuilder aggregation =
                AggregationBuilders
                        .stats("agg")
                        .field("price");

        searchSourceBuilder.aggregation(aggregation);


        SearchRequest searchRequest = new SearchRequest("posts");

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = highLevelClient.search(searchRequest,RequestOptions.DEFAULT);

        searchResponse.status();
        searchResponse.getTook();
        searchResponse.getTotalShards();
        SearchHits hits=searchResponse.getHits();
        hits.getTotalHits();

        Stats agg = searchResponse.getAggregations().get("agg");
        double min = agg.getMin();
        double max = agg.getMax();
        double avg = agg.getAvg();
        double sum = agg.getSum();
        long count = agg.getCount();

       /* Map<String, Aggregation> aggMap = searchResponse.getAggregations().asMap();
        Terms teamAgg= (Terms) aggMap.get("name_count");

        for (Terms.Bucket entry : teamAgg.getBuckets()) {
            Object key = entry.getKey();
            long docCount = entry.getDocCount();
            //得到所有子聚合
            Avg price_avg = entry.getAggregations().get("price_avg");
            Sum price_sum = entry.getAggregations().get("price_sum");
            double avgValue = price_avg.getValue();
            double sumValue = price_sum.getValue();
            System.out.println("名字："+String.valueOf(key)+"个数："+docCount+"平均值："+String.valueOf(avgValue)+"和为：："+String.valueOf(sumValue));
        }*/


    }

    @RequestMapping("analyze")
    public void analyze()throws Exception{

//        Map<String, Object> stopFilter = new HashMap<>();
//        stopFilter.put("type", "stop");
//        stopFilter.put("stopwords", new String[]{ "to" });
//        AnalyzeRequest request = AnalyzeRequest.buildCustomAnalyzer("standard")
//                .addCharFilter("html_strip")
//                .addTokenFilter("lowercase")
//                .addTokenFilter(stopFilter)
//                .build("<b>Some text to analyze</b>");

//        AnalyzeRequest request = AnalyzeRequest.withIndexAnalyzer(
//                "my_index",
//                "my_analyzer",
//                "some text to analyze"
//        );



        RestHighLevelClient highLevelClient= EsRestHighUtil.getRestClient();

        AnalyzeRequest request = AnalyzeRequest.withGlobalAnalyzer("english",
                "Some text to analyze", "Some more text to analyze");

        AnalyzeResponse response = highLevelClient.indices().analyze(request, RequestOptions.DEFAULT);

        List<AnalyzeResponse.AnalyzeToken> tokens = response.getTokens();

        for(AnalyzeResponse.AnalyzeToken t : tokens){
            int endOffset = t.getEndOffset();
            int position = t.getPosition();
            int positionLength = t.getPositionLength();
            int startOffset = t.getStartOffset();
            String term = t.getTerm();
            String type = t.getType();
            System.out.println("Start:" + startOffset + ",End:" + endOffset + ",Position:" + position + ",Length:" + positionLength +
                    ",Term:" + term + ",Type:" + type);
        }


    }


    public  void orderOkPercent(){
        SearchRequest request = new SearchRequest("post");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("time");
        rangeQueryBuilder.gte("");
        rangeQueryBuilder.lte("");
        boolQueryBuilder.must(rangeQueryBuilder);

        //日期直方图聚合
        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = AggregationBuilders.dateHistogram("day_order");
        dateHistogramAggregationBuilder.field("time");


        searchSourceBuilder.query(boolQueryBuilder).aggregation(dateHistogramAggregationBuilder);
        request.source(searchSourceBuilder);



        //统计每天有多少条数据,以及某字段的平均值。
        AvgAggregationBuilder avgAggregationBuilder = AggregationBuilders
                .avg("avg_aggsName")
                .field("fieldName");

        DateHistogramAggregationBuilder subAggregation = AggregationBuilders
                .dateHistogram("aggsName")
                .field("fieldName") //可以是time
                .dateHistogramInterval(DateHistogramInterval.DAY)
                .format("yyyy-MM-dd")
                .minDocCount(0L)
                .subAggregation(avgAggregationBuilder);




    }

}
