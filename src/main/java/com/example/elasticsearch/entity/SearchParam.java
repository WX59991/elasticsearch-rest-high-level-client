package com.example.elasticsearch.entity;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author wangxia
 * @date 2019/9/10 15:03
 * @Description:  请求参数类
 */
public class SearchParam {

    private int pageNo;

    private int pageSize;

    /**
     * key--检索字段  value--检索值
     */
    private Map<String, Object> param;

    /**
     * key--排序字段 value--排序规则
     */
    private Map<String, SortOrder> sort;


    /**
     * 分类统计 key--分类统计字段 value--分类统计结果字段 分类统计类型
     */
    private Map<String, Map<String, AggEnum>> agg;


    private BoolQueryBuilder boolQueryBuilders;

    private String query;

    /**
     * 创建bool查询条件
     *
     * @return
     */
    public Optional<BoolQueryBuilder> getBoolQueryBuilder() {
        if (param != null && param.size()>0) {
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            param.forEach((t, p) -> {
                boolQueryBuilder.must(QueryBuilders.matchQuery(t, p));
            });
            return Optional.of(boolQueryBuilder);
        }
        return Optional.empty();
    }


    //--------------------------------------get/set-------------------------

    public List<AggregationBuilder> getAggregation() {
        List<AggregationBuilder> aggregationBuilders=new ArrayList<>();
        if(agg!=null){
            agg.keySet().stream()
                    .filter(k -> !StringUtils.isEmpty(k))
                    .forEach(k -> {
                        agg.get(k).forEach((k2, v) -> {
                            if (!StringUtils.isEmpty(k2)) {
                                createAggregationBuilder(k,k2,v).ifPresent(agg->aggregationBuilders.add(agg));
                            }
                        });
                    });
        }
        return aggregationBuilders;
    }

    public Optional<AggregationBuilder> createAggregationBuilder(String filed, String newField, AggEnum aggEnum) {
        switch (aggEnum) {
            case AVG:
                return Optional.of(AggregationBuilders.avg(newField).field(filed));
            case SUM:
                return Optional.of(AggregationBuilders.sum(newField).field(filed));
            case MAX:
                return Optional.of(AggregationBuilders.max(newField).field(filed));
            case MIN:
                return Optional.of(AggregationBuilders.min(newField).field(filed));
            case COUNT:
                return Optional.of(AggregationBuilders.count(newField).field(filed));
            case TERM:
                return Optional.of(AggregationBuilders.terms(newField).field(filed));
            default:
                return Optional.empty();
        }
    }

    public void setAgg(Map<String, Map<String, AggEnum>> agg) {
        this.agg = agg;
    }

    public void setPageNo(int pageNo) {
        if (pageNo <= 1) {
            this.pageNo = 1;
            return;
        }
        this.pageNo = pageNo;
    }

    public int getPageNo() {
        return pageNo;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        if (pageSize <= 1) {
            this.pageSize = 0;
            return;
        }
        this.pageSize = pageSize;
    }

    public Map<String, Object> getParam() {
        return param;
    }

    public void setParam(Map<String, Object> param) {
        this.param = param;
    }

    public List<SortBuilder> getSortBuilder() {
        List<SortBuilder> result = new ArrayList<>();
        if(sort!=null){
            sort.forEach((t, v) -> {
                result.add(new FieldSortBuilder(t).order(v));
            });
        }
        return result;
    }

    public void setSorr(Map<String, SortOrder> sort) {
        this.sort = sort;
    }
    public Optional<QueryStringQueryBuilder> getQuery() {
        if(!StringUtils.isEmpty(query)){
            return Optional.of(new QueryStringQueryBuilder(query));
        }
        return Optional.empty();
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public Optional<BoolQueryBuilder> getBoolQueryBuilders() {
        if(boolQueryBuilders==null){
            return Optional.empty();
        }
        return Optional.of(boolQueryBuilders);
    }

    public void setBoolQueryBuilders(BoolQueryBuilder boolQueryBuilders) {
        this.boolQueryBuilders = boolQueryBuilders;
    }

}
