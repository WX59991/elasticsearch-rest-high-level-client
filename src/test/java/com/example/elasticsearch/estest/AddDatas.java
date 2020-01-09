package com.example.elasticsearch.estest;

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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

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

    String index = "eos-order-test";

    @Test
    public void adds(){
        JSONObject jsonObject=getData();
       for(int i=0;i<100;i++){
           new Thread(new SaveThread(index,elasticSearchUtils,jsonObject)).start();
       }
       while (true){}
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
        param.put("_id", "2059102845809664");
        searchParam.setParam(param);
        try {
            SearchResponse searchResponse = elasticSearchUtils.search("eos-order-591028", searchParam);
            SearchHit[] datas=searchResponse.getHits().getHits();
            if(datas==null || datas.length==0)
                throw  new NullPointerException("数据不存在");
            SearchHit data=datas[0];
            return JSONObject.parseObject(data.getSourceAsString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}

class SaveThread implements Runnable{
    private static Long BEGING=1_0000_0000l;

    ElasticSearchUtils elasticSearchUtils;

    String index;
    JSONObject jsonObject;

    public SaveThread(String index,ElasticSearchUtils elasticSearchUtils,JSONObject jsonObject){
        this.elasticSearchUtils=elasticSearchUtils;
        this.index=index;
        this.jsonObject=jsonObject;
    }

    @Override
    public void run() {
        saveDatas(jsonObject);
    }

    /**
     * 批量插入数据
     */
    public void saveDatas(JSONObject jsonObject) {
        try {
            while (true){
                List<JSONObject> datas = new ArrayList<>();
                for(int i=0;i<10;i++){
                    JSONObject temp=JSONObject.parseObject(jsonObject.toJSONString());
                    String newId="1319062"+(getId());
                    temp.getJSONObject("orderBaseInfo").put("orderId",newId);
                    String data=LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    temp.getJSONObject("orderBaseInfo").put("updateTime",data);
                    temp.getJSONObject("orderBaseInfo").put("createTime",data);
                    datas.add(temp);
                }
                //批量插入
                BulkResponse response=elasticSearchUtils.bulkSave(index, datas);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static synchronized long getId(){
        return BEGING++;
    }

    public void saveData2() {
        try {
            while (true){
                //批量插入
                elasticSearchUtils.insert(index,createDate(),null);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public JSONObject createDate(){
        JSONObject temp=new JSONObject();
        String name="zhangsan"+(getId());
        temp.put("username",name);
        Random random=new Random();
        int age=random.nextInt(1000)+10;
        temp.put("age",age);
//        String data=LocalDateTime.now().minusHours(random.nextInt(18)+1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String data=LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        temp.put("createDate",data);
        return temp;
    }
}

