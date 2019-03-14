package com.example.kafka.demo.elastic;

import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ElasticClient {
    private static RestHighLevelClient client;
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

    public static SearchHits searchWhere(String index, String type, String custId) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(type);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("cust_id", custId));
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


}
