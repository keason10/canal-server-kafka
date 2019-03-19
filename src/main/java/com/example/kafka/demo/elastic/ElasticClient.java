package com.example.kafka.demo.elastic;

import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateResponse;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//官方文档 https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high.html
public class ElasticClient {
    private static RestHighLevelClient client;
    private TransportClient tclient;
    static  {
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost("192.168.64.128", 9200, "http"));
        client = new RestHighLevelClient(restClientBuilder);
    }

    public static IndexResponse insert(String index, String type, String docId, String dataJson) throws IOException {
        IndexRequest request = new IndexRequest(index, type, docId);
        request.source(dataJson, XContentType.JSON);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        return response;
    }

    public static GetResponse search(String index, String type, String docId, String dataJson) throws IOException {
        GetRequest request = new GetRequest(index, type, docId).version(2);
        GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);
        return getResponse;
    }

    //通过单个键名称 和对应的键值查询数据
    public static SearchHits searchWhere(String index, String type, String keyName,Object keyValue) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(type);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(keyName, keyValue));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        if (searchResponse.status() == RestStatus.OK) {
            return searchResponse.getHits();

        }
        return null;
    }

    public static UpdateResponse updateWithJson(String index, String type, String docId, String dataJson) throws IOException {
        UpdateRequest request = new UpdateRequest(index, type, docId);
        request.doc(dataJson, XContentType.JSON);
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        return response;
    }

    public static UpdateResponse updateWithBuilder(String index, String type, String docId, XContentBuilder builder) throws IOException {
        UpdateRequest request = new UpdateRequest(index, type, docId);
        request.doc(builder);
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        return response;
    }



    public static DeleteResponse delete(String index, String type, String docId) throws IOException {
        DeleteRequest request = new DeleteRequest(index, type, docId);
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        return response;
    }

    //param 不支支持嵌套对象
    //根据param 中的 key1=value1 and key2= value2 。。。组装之后 查询 index/type 返回数据
    public static SearchTemplateResponse mutiAndSearch(String index, String type,Map<String,Object> param) throws IOException {
        if (param == null) {
            return null;
        }
        SearchTemplateRequest request = new SearchTemplateRequest();
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(type);
        request.setRequest(searchRequest);

        //拼装动态变量用{{}} 包围 详情参考 https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-search-template.html
        request.setScriptType(ScriptType.INLINE);
        Map map = new HashMap();
        Map mapBool = new HashMap<>();
        Map mapBoolStr = new HashMap<>();
        List<Map> mustArray = new ArrayList<>();
        for (Map.Entry<String, Object> entry : param.entrySet()) {
            Map<String, Map> mapMap = new HashMap<>();
            Map<String, Object> mapParam = new HashMap<>();
            mapParam.put(entry.getKey(), String.format("{{%s}}", entry.getKey()));
            mapMap.put("match", mapParam);
            mustArray.add(mapMap);
        }
        mapBoolStr.put("must", mustArray);
        mapBool.put("bool", mapBoolStr);
        map.put("query", mapBool);
        request.setScript(JSON.toJSONString(map));

        //设置变量值
        request.setScriptParams(param);
        SearchTemplateResponse response = client.searchTemplate(request, RequestOptions.DEFAULT);
        //返回值字段参考 response.json
        return response;
    }

    //根据 keyName = ${keyValue} 查询 index/type 找到对应记录，用param 参数进行更新
    public static BulkByScrollResponse updateKeyWithDoc(String index,String type,String keyName,Object keyValue,Map<String,Object> param) throws IOException {
        UpdateByQueryRequest request = new UpdateByQueryRequest(index);
        request.setDocTypes(type);
        request.setQuery(QueryBuilders.matchQuery(keyName, keyValue));
        StringBuilder str = new StringBuilder();
        for (Map.Entry<String, Object> entity : param.entrySet()) {
            str.append("ctx._source.").append(entity.getKey()).append("=params.").append(entity.getKey()).append(";");
        }
        request.setScript(new Script(ScriptType.INLINE, "painless", str.toString(), param));
        BulkByScrollResponse bulkResponse = client.updateByQuery(request, RequestOptions.DEFAULT);
        //返回值字段参考 bulkResponse.json 文件
        return bulkResponse;
    }



    public static void main(String[] args) throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("mobile", "111111111111");
        map.put("note", "1111111");
        map.put("location", "what a fuck method");
        updateKeyWithDoc("test", "order", "cust_id", "1", map);


        mutiAndSearch("test", "order", map);
    }

}
