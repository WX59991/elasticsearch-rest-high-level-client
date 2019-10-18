package com.example.elasticsearch;

import com.alibaba.fastjson.JSONObject;
import com.example.elasticsearch.entity.SearchParam;
import com.example.elasticsearch.utils.ElasticSearchUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author wangxia
 * @date 2019/9/12 14:49
 * @Description:
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class TestElasticSearch {

    @Autowired
    ElasticSearchUtils elasticSearchUtils;

    String index = "blog3";

    @Test
    public void addDate() {
        Arrays.asList(point.split("\\|"))
                .stream()
                .filter(v -> !StringUtils.isEmpty(v))
                .forEach(v -> {
                    String[] locations = v.split(",");
                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put("lon", locations[0]);
                    jsonObject.put("lat", locations[1]);
                    JSONObject loaction = new JSONObject();
                    loaction.put("location", jsonObject);
                    loaction.put("address","BJ");
                    try {
                        //插入单条
                        System.out.println(elasticSearchUtils.insert(index, loaction));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
    }

    @Test
    public void update(){
        SearchParam searchParam=new SearchParam();
        searchParam.setPageNo(0);
        searchParam.setPageSize(100);
        List<JSONObject> newDatas=new ArrayList<>();
        try{
            SearchResponse searchResponse=elasticSearchUtils.search(index,searchParam);
            Arrays.asList(searchResponse.getHits().getHits()).stream()
                    .forEach(hit->{
                        String id=hit.getId();
                        JSONObject data=JSONObject.parseObject(hit.getSourceAsString());
                        data.put("address","河南省");
                        try{
                            elasticSearchUtils.update(index,id,data);
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                    });
//            elasticSearchUtils.blukUpdate(index,newDatas);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    String point = "116.411087,39.971775|116.246087,39.917786|116.636454,39.888118|116.411087,39.971775|116.246087,39.917786|116.636454,39.888118|116.360872,39.922172|116.367879,39.922684|116.367555,39.918284|116.361124,39.918008|116.361124,39.918008|119.378826,43.978955|119.369125,43.977502|119.383605,43.972909";

}
