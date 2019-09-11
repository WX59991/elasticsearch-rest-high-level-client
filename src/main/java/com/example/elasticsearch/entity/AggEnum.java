package com.example.elasticsearch.entity;

/**
 * @author wangxia
 * @date 2019/9/10 15:36
 * @Description:  分类统计类型
 */
public enum AggEnum {

    /**
     * 求平均值
     */
    AVG("avg"),
    /**
     * 求和
     */
    SUM("sum"),
    /**
     * 统计出现的次数
     */
    COUNT("count"),
    /**
     * 求最大值
     */
    MAX("max"),
    /**
     * 求最小值
     */
    MIN("min"),
    /**
     * 进行分类统计
     */
    TERM("term");


    private String val;

    AggEnum(String val){
        this.val=val;
    }

    public String value(){
        return val;
    }

}
