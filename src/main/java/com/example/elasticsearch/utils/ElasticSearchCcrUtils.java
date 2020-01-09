package com.example.elasticsearch.utils;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.ccr.*;
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.core.BroadcastResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * @author wangxia
 * @date 2019/11/28 9:48
 * @Description:  es跨集群复制
 */
@Component
public class ElasticSearchCcrUtils {

    @Autowired
    RestHighLevelClient highLevelClient;

    /**
     * 创建跟随索引
     * @param remoteCluster  远程集群名
     * @param leaderIndex    领导者索引名
     * @param followerIndex  跟随者索引名
     * @return  是否创建跟随成功
     * @throws IOException  可能抛出io异常
     */
    public boolean createFollower(String remoteCluster, String leaderIndex, String followerIndex) throws IOException {
        return  createFollower(remoteCluster,leaderIndex,followerIndex,-1);
    }

    /**
     * 创建索引
     * @param remoteCluster  远程集群名
     * @param leaderIndex    领导者索引名
     * @param followerIndex  跟随者索引名
     * @param activeShardCount  等待响应的分片数
     * @return 是否创建跟随成功
     * @throws IOException  可能抛出io异常
     */
    public boolean createFollower(String remoteCluster, String leaderIndex, String followerIndex,int activeShardCount) throws IOException {
        PutFollowRequest putFollowRequest=null;
        if(activeShardCount>0){
            ActiveShardCount activeShardCountTmp=ActiveShardCount.from(activeShardCount);
            putFollowRequest=new PutFollowRequest(remoteCluster,leaderIndex,followerIndex,activeShardCountTmp);
        }else{
            putFollowRequest=new PutFollowRequest(remoteCluster,leaderIndex,followerIndex);
        }
        PutFollowResponse putFollowResponse=highLevelClient.ccr().putFollow(putFollowRequest, RequestOptions.DEFAULT);
        return putFollowResponse.isFollowIndexCreated();
    }

    /**
     * 删除自动跟随
     * @param follwerName  跟随的名称
     * @return 是否成功
     * @throws IOException  抛出的异常
     */
    public boolean deleteAutoFollower(String follwerName) throws IOException{
        DeleteAutoFollowPatternRequest deleteAutoFollowPatternRequest=new DeleteAutoFollowPatternRequest(follwerName);
        AcknowledgedResponse acknowledgedResponse=highLevelClient.ccr().deleteAutoFollowPattern(deleteAutoFollowPatternRequest,RequestOptions.DEFAULT);
        return acknowledgedResponse.isAcknowledged();
    }

    /**
     * 删除跟随
     * @param followerIndex  接收方的index
     * @return true--删除成功 false--删除失败
     * @throws IOException  异常
     */
    public boolean deleteFollower(String followerCluster, String followerIndex, String leaderRemoteCluster) throws IOException{
        UnfollowRequest unfollowRequest=new  UnfollowRequest(followerIndex);
        GetSettingsRequest getSettingsRequest=new GetSettingsRequest().indices(followerIndex);
        GetSettingsResponse getSettingsResponse=highLevelClient.indices().getSettings(getSettingsRequest,RequestOptions.DEFAULT);
        String uuid=getSettingsResponse.getSetting(followerIndex,"index.uuid");
        ForgetFollowerRequest forgetFollowerRequest=new ForgetFollowerRequest(followerCluster,followerIndex,uuid,leaderRemoteCluster,followerIndex);
        BroadcastResponse broadcastResponse=highLevelClient.ccr().forgetFollower(forgetFollowerRequest,RequestOptions.DEFAULT);
        AcknowledgedResponse acknowledgedResponse=highLevelClient.ccr().unfollow(unfollowRequest,RequestOptions.DEFAULT);
        return acknowledgedResponse.isAcknowledged();
    }
}
