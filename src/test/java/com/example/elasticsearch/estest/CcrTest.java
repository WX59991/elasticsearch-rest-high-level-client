package com.example.elasticsearch.estest;

import com.example.elasticsearch.utils.ElasticSearchCcrUtils;
import com.example.elasticsearch.utils.ElasticSearchUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author wangxia
 * @date 2019/11/28 10:25
 * @Description:
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class CcrTest {

    @Autowired
    ElasticSearchCcrUtils elasticSearchCcrUtils;

    @Autowired
    ElasticSearchUtils elasticSearchUtils;

    @Test
    public void getAliaas(){
        String indexPattern="eos-order-*";
        try {
            elasticSearchUtils.getIndexs(indexPattern).forEach(index->{
                System.out.print("\""+index+"\",");
            });
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void createFollower(){
        String remoteCluster="";
        List<String> leaderAlias= Arrays.asList();
        leaderAlias.forEach(index->{
            try {
                elasticSearchCcrUtils.createFollower(remoteCluster,index,index,1);
            }catch (IOException e){
                e.printStackTrace();
            }
        });
    }

    @Test
    public void deleteAutoFollwer(){
        try {
//            String autoFollwer="test的索引";
//            String res=elasticSearchCcrUtils.deleteAutoFollower(autoFollwer)?"删除成功":"删除失败";
//            System.out.println("自动跟随索引:["+autoFollwer+"]"+res);
            String indexPattern="test-*";
            String follwerCluster="elastic";
            String leaderCluster="elastic-bak";
            elasticSearchUtils.getIndexs(indexPattern).forEach(index->{
                try {
                    String res2= elasticSearchCcrUtils.deleteFollower(follwerCluster,index,leaderCluster)?"删除成功":"删除失败";
                    System.out.println("索引["+index+"]"+res2);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
