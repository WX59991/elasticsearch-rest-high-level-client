package com.example.elasticsearch;

import com.alibaba.fastjson.JSONObject;
import com.example.elasticsearch.entity.SearchParam;
import com.example.elasticsearch.utils.ElasticSearchUtils;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
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

/**
 * @author wangxia
 * @date 2019/10/18 16:19
 * @Description:
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class AddDatas {

    @Autowired
    ElasticSearchUtils elasticSearchUtils;

    String index = "eos_test";

    @Test
    public void adds(){
        JSONObject jsonObject=getData();
        saveDatas(jsonObject);
    }

    /**
     * 批量插入数据
     */
    public void saveDatas(JSONObject jsonObject) {
        try {
            Long begin=1_0002_6749l;
            while (true){
                List<JSONObject> datas = new ArrayList<>();
                for(int i=0;i<5;i++){
                    JSONObject temp=JSONObject.parseObject(jsonObject.toJSONString());
                    String newId="1319062"+(begin++);
                    temp.getJSONObject("orderBaseInfo").put("orderId",newId);
                    datas.add(temp);
                }
                //批量插入
                BulkResponse response=elasticSearchUtils.bulkSave(index, datas);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据Id查询出数据
     * @return
     */
    public JSONObject getData(){
        SearchParam searchParam = new SearchParam();
        searchParam.setPageNo(1);
        searchParam.setPageSize(10);
        Map<String, Object> param = new HashMap<>();
        param.put("_id", "FuIE3m0B4NLAvpKWyiR9");
        searchParam.setParam(param);
        try {
            SearchResponse searchResponse = elasticSearchUtils.search(index, searchParam);
            SearchHit[] datas=searchResponse.getHits().getHits();
            if(datas==null)
                throw  new NullPointerException("数据不存在");
            SearchHit data=datas[0];
            return JSONObject.parseObject(data.getSourceAsString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
