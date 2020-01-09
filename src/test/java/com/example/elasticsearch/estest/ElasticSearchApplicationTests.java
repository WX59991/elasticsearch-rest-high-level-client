package com.example.elasticsearch.estest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.elasticsearch.entity.AggEnum;
import com.example.elasticsearch.entity.SearchParam;
import com.example.elasticsearch.utils.ElasticSearchUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ElasticSearchApplicationTests {

    @Autowired
    ElasticSearchUtils elasticSearchUtils;

    String index = "eos";

    /**
     * 测试创建索引
     */
    @Test
    public void testCreateIndex() {
        try {
            elasticSearchUtils.createIndex(index);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试保存数据
     */
    @Test
    public void testSave() {
        try {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("address", "116.550316:39.924288");
            jsonObject.put("remark","测试数据");
            //插入单条
            elasticSearchUtils.insert(index, jsonObject,null);
            List<JSONObject> datas = new ArrayList<>();
            JSONObject jsonObject2 = new JSONObject();
            jsonObject2.put("address", "116.551179:39.933031");
            datas.add(jsonObject2);
            JSONObject jsonObject3 = new JSONObject();
            jsonObject3.put("address", "116.54931:39.943431");
            datas.add(jsonObject3);
            //批量插入
            elasticSearchUtils.bulkSave(index, datas);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试更新数据
     */
    @Test
    public void testUpdate() {
        try {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("age", 18);
            //更新单条
            elasticSearchUtils.update(index, "lZMiGW0Bx2-tjtKxeH_B", jsonObject);
            List<JSONObject> datas = new ArrayList<>();
            JSONObject jsonObject2 = new JSONObject();
            jsonObject2.put("id", "5sghGW0Blxl66DRzz_s6");
            jsonObject2.put("age", 18);
            datas.add(jsonObject2);
            JSONObject jsonObject3 = new JSONObject();
            jsonObject3.put("id", "EMgiGW0Blxl66DRzeP0v");
            jsonObject3.put("age", 18);
            datas.add(jsonObject3);
            //批量更新
            elasticSearchUtils.blukUpdate(index, datas);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试简单搜索
     */
    @Test
    public void testSearch() {
        SearchParam searchParam = new SearchParam();
        searchParam.setPageNo(1);
        searchParam.setPageSize(10);
        Map<String, Object> param = new HashMap<>();
        param.put("_id", "1319062508677791");
        searchParam.setParam(param);
//        Map<String, SortOrder> sort = new HashMap<>();
//        sort.put("_id", SortOrder.DESC);
//        searchParam.setSorr(sort);
        try {
            SearchResponse searchResponse = elasticSearchUtils.search(index, searchParam);
            System.out.println("检索结果：" + JSON.toJSONString(searchResponse.getHits()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试自定义检索
     */
    @Test
    public void testStringQuery(){
        SearchParam searchParam = new SearchParam();
        searchParam.setPageNo(1);
        searchParam.setPageSize(10);
        searchParam.setQuery("_id:5sghGW0Blxl66DRzz_s6");
        try {
            SearchResponse searchResponse = elasticSearchUtils.search(index, searchParam);
            System.out.println("检索结果：" + JSON.toJSONString(searchResponse.getHits()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 分类统计
     */
    @Test
    public void testSearchAgg() {
        SearchParam searchParam = new SearchParam();
        //分类统计
        Map<String, Map<String, AggEnum>> agg=new HashMap<>();
        Map<String,AggEnum> newfield=new HashMap<>();
        newfield.put("add_count",AggEnum.COUNT);
        agg.put("_id",newfield);
        searchParam.setAgg(agg);
        try {
            SearchResponse searchResponse = elasticSearchUtils.search(index, searchParam);
            System.out.println("检索结果：" + JSON.toJSONString(searchResponse.getAggregations()));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 测试通过id删除数据
     */
    @Test
    public void deleteById(){
        try {
            elasticSearchUtils.deleteById(index,"EMgiGW0Blxl66DRzeP0v");
        }catch (IOException e){
            e.printStackTrace();
        }

    }

    /**
     * 测试通过检索语句删除
     */
    @Test
    public void deleteByQuery(){
        Map<String, Object> param = new HashMap<>();
        param.put("location", "*");
        try{
            elasticSearchUtils.deleteByQuery(index,param);
        }catch (Exception e){
            e.printStackTrace();
        }
    }




}
