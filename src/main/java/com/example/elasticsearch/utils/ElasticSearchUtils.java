package com.example.elasticsearch.utils;

import com.alibaba.fastjson.JSONObject;
import com.example.elasticsearch.entity.SearchParam;
import com.sun.istack.internal.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author wangxia
 * @date 2019/9/10 9:58
 * @Description: es操作工具类
 * 在es中插入成功响应码为201
 * 更新成功响应码为200
 */
@Slf4j
@Component
public class ElasticSearchUtils {

    @Autowired
    RestHighLevelClient highLevelClient;

    private final String ID_FIELD = "id";

    /**
     * 创建索引
     *
     * @param index
     * @return
     * @throws IOException
     */
    public boolean createIndex(@NotNull String index) throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
        CreateIndexResponse createIndexResponse = highLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        return createIndexResponse.isAcknowledged();
    }

    /**
     * 删除索引，删除索引时相关数据也会被删除
     *
     * @param index
     * @return
     * @throws IOException
     */
    public boolean deleteIndex(@NotNull String index) throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
        AcknowledgedResponse acknowledgedResponse = highLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        return acknowledgedResponse.isAcknowledged();
    }

    public List<String> getIndexs(String indexPattern) throws  IOException{
        GetIndexRequest getIndexRequest=new GetIndexRequest(indexPattern);
        GetIndexResponse getIndexResponse=highLevelClient.indices().get(getIndexRequest,RequestOptions.DEFAULT);
        return Arrays.asList(getIndexResponse.getIndices());
    }


    /**
     * 插入单条数据  如果不指定id数据会重复插入
     *
     * @param index
     * @param jsonObject
     * @return
     * @throws IOException
     */
    public IndexResponse insert(@NotNull String index, @NotNull JSONObject jsonObject, String id) throws IOException {
        XContentBuilder builder = createContentBuilder(jsonObject).get();
        IndexRequest request = null;
        if (!StringUtils.isEmpty(id)) {
            request = new IndexRequest(index).source(builder).id(id);
        } else {
            request = new IndexRequest(index).source(builder);
        }
        //写入之后立即刷新索引，使数据可以及时查询到
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        IndexResponse indexResponse = highLevelClient.index(request, RequestOptions.DEFAULT);
        return indexResponse;
    }

    public int insertWithId(@NotNull String index, @NotNull String id, @NotNull JSONObject jsonObject) throws IOException {
        XContentBuilder builder = createContentBuilder(jsonObject).get();
        IndexRequest request = new IndexRequest(index).source(builder).id(id);
        IndexResponse indexResponse = highLevelClient.index(request, RequestOptions.DEFAULT);
        return indexResponse.status().getStatus() == 201 ? 1 : 0;
    }

    /**
     * 批量插入
     *
     * @param index       索引
     * @param jsonObjects 数据列表
     * @return 返回值中包含响应结果，成功失败记录数等
     * @throws IOException
     */
    public BulkResponse bulkSave(@NotNull String index, @NotNull List<JSONObject> jsonObjects) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        jsonObjects.stream()
                .map(this::createContentBuilder)
                .collect(Collectors.toList()).forEach(v -> {
            v.ifPresent(val -> bulkRequest.add(new IndexRequest(index).source(val)));
        });
        return highLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
    }

    /**
     * 更新单条数据
     *
     * @param index      索引
     * @param id
     * @param jsonObject 更新数据
     * @return
     * @throws IOException
     */
    public int update(@NotNull String index, @NotNull String id, @NotNull JSONObject jsonObject) throws IOException {
        XContentBuilder builder = createContentBuilder(jsonObject).get();
        UpdateRequest updateRequest = new UpdateRequest(index, id).doc(builder);
        UpdateResponse updateResponse = highLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        return updateResponse.status().getStatus() == 200 ? 1 : 0;
    }

    /**
     * 批量更新，传入的更新数据必须包含id字段
     *
     * @param index
     * @param jsonObjects 数据必须包含id字段
     * @return
     */
    public BulkResponse blukUpdate(@NotNull String index, @NotNull List<JSONObject> jsonObjects) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        jsonObjects.stream()
                .map(v -> createUpdateRequest(index, v))
                .collect(Collectors.toList()).forEach(v -> {
            v.ifPresent(val -> bulkRequest.add(val));
        });
        return highLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
    }

    /**
     * 通过id删除
     *
     * @param index 索引
     * @param id    要删除数据id
     * @return
     * @throws IOException
     */
    public int deleteById(@NotNull String index, @NotNull String id) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(index, id);
        DeleteResponse deleteResponse = highLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        return deleteResponse.status().getStatus() == 200 ? 1 : 0;
    }

    /**
     * 通过检索语句删除数据
     *
     * @param index 索引
     * @param query 检索语句
     * @return
     * @throws IOException
     */
    public long deleteByQuery(@NotNull String index, @NotNull Map<String, Object> query) throws IOException {
        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(index);
        SearchParam searchParam = new SearchParam();
        searchParam.setParam(query);
        searchParam.getBoolQueryBuilder().ifPresent(v -> deleteByQueryRequest.setQuery(v));
        BulkByScrollResponse bulkByScrollResponse = highLevelClient.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
        return bulkByScrollResponse.getDeleted();
    }

    /**
     * 条件检索
     *
     * @param index       索引
     * @param searchParam 检索条件
     * @return
     * @throws IOException
     */
    public SearchResponse search(@NotNull String index, @NotNull SearchParam searchParam) throws IOException {
        SearchRequest searchRequest = createSearchRequest(index, searchParam);
        return highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    private Optional<UpdateRequest> createUpdateRequest(String index, JSONObject jsonObject) {
        String id = jsonObject.remove(ID_FIELD).toString();
        jsonObject.remove(ID_FIELD);
        Optional.empty();
        XContentBuilder builder = createContentBuilder(jsonObject).get();
        return Optional.of(new UpdateRequest(index, id).doc(builder));
    }

    /**
     * @param jsonObject
     * @return 为空时进行get会抛出异常
     */
    private Optional<XContentBuilder> createContentBuilder(JSONObject jsonObject) {
        try {
            if (jsonObject == null) {
                return null;
            }
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            for (Map.Entry temp : jsonObject.entrySet()) {
                builder.field(temp.getKey().toString(), temp.getValue());
            }
            return Optional.of(builder.endObject());
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    private SearchRequest createSearchRequest(String index, SearchParam searchParam) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(searchParam.getPageSize());
        searchSourceBuilder.from((searchParam.getPageNo() - 1) * searchParam.getPageSize());
        //添加检索条件
        searchParam.getBoolQueryBuilder().ifPresent(v -> searchSourceBuilder.query(v));
        //添加排序条件
        searchParam.getSortBuilder().forEach(v -> searchSourceBuilder.sort(v));
        //添加分类统计
        searchParam.getAggregation().forEach(agg -> searchSourceBuilder.aggregation(agg));
        //自定义bool检索条件
        searchParam.getBoolQueryBuilders().ifPresent(v -> searchSourceBuilder.query(v));
        //自定义query检索条件
        searchParam.getQuery().ifPresent(squery -> searchSourceBuilder.query(squery));
        SearchRequest searchRequest = new SearchRequest();
        return searchRequest.indices(index).source(searchSourceBuilder);
    }
}
