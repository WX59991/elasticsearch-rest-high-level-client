package com.example.elasticsearch.estest;

import com.alibaba.fastjson.JSONObject;
import com.example.elasticsearch.entity.SearchParam;
import com.example.elasticsearch.utils.ElasticSearchUtils;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wangxia
 * @date 2019/12/30 15:03
 * @Description:
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class ExitsTest {

    @Autowired
    ElasticSearchUtils elasticSearchUtils;

    String index = "weos-order-test";

    @Test
    public void adds(){
        JSONObject jsonObject=JSONObject.parseObject(data);
        for(int i=0;i<100;i++){
            new Thread(new SaveThread2(index,elasticSearchUtils,jsonObject)).start();
        }
        while (true){}
    }


    String data="{\"orderBaseInfo\":{\"businessType\":\"08\",\"cancelTag\":\"0\",\"cancelType\":\"0\",\"cardType\":\"19\",\"cityCode\":\"110\",\"claimFlag\":\"0\",\"connChannel\":\"10\",\"couMoney\":\"0\",\"createTime\":\"2019-11-01 01:36:37\",\"custId\":\"999993053115965\",\"custIp\":\"218.241.251.218\",\"delayTime\":\"2019-11-01 01:36:37\",\"deliverCompanyCode\":\"1042\",\"deliverDateType\":\"01\",\"deliverTypeCode\":\"99\",\"incomeMoney\":\"0\",\"isOfflineFlow\":\"\",\"merchantId\":\"1100000\",\"operateSwitch\":\"1\",\"orderFrom\":\"MOBILE\",\"orderId\":\"1859110106291456\",\"orderNo\":\"108169005056\",\"orderSourceFlag\":\"2\",\"orderState\":\"CA\",\"orderTotalMoney\":\"0\",\"orgCustId\":\"999993053115965\",\"orgPrice\":\"0\",\"payCompleteTime\":\"2019-11-01 01:36:37\",\"payState\":\"1\",\"payType\":\"01\",\"payWay\":\"01\",\"postFee\":\"0\",\"postTag\":\"1\",\"processDelayTime\":\"2019-11-01 01:36:15\",\"processMerchId\":\"1100000\",\"provinceCode\":\"11\",\"releaseTime\":\"2019-11-01 01:36:37\",\"sourceFlag\":\"2\",\"staffId\":\"\",\"stateSynTag\":\"\",\"synTag\":\"\",\"topayMoney\":\"0\",\"updateTime\":\"2019-11-01 02:21:16\"},\"orderChannelInfo\":{\"kingCardChannelInfo\":{\"channel\":\"9876\",\"channelThree\":\"9999\",\"channelTwo\":\"9999\"}},\"orderFeeInfo\":[{\"fee\":\"0\",\"feeTypeCode\":\"2001\",\"realFee\":\"0\",\"reliefFee\":\"0\",\"toPayFee\":\"0\"},{\"fee\":\"0\",\"feeTypeCode\":\"3001\",\"realFee\":\"0\",\"reliefFee\":\"0\",\"toPayFee\":\"0\"}],\"orderGoodsInfo\":{\"goodsBaseInfo\":{\"goodsId\":\"111702278573\",\"goodsInstId\":\"1859110106291457\",\"goodsName\":\"腾讯天王卡\",\"productDesc\":\"腾讯天王卡： 月费59元（按天扣费），套内：定向流量T应用流量全免，500分钟国内语音，国内接听免费，赠送来电显示；套外：国内语音0.1元/分钟，短彩信0.1元/条，省内流量默认开通“流量日租宝”功能1元/500MB省内流量（当日有效，不足10M按0.1元/M计费）用完自动叠加，按量计费。省外流量超出后2元/500M省外流量（当日有效，不足20M按0.1元/M计费）用完自动叠加，按量计费。\",\"productId\":\"90155946\",\"productName\":\"腾讯天王卡\",\"productValue\":\"59\",\"tmplId\":\"60000028\"},\"goodsInsAtval\":[{\"attrCode\":\"A000003\",\"attrName\":\"号码网别\",\"attrValCode\":\"A000003V000003\",\"attrValName\":\"4G\"},{\"attrCode\":\"A000004\",\"attrName\":\"付费类型\",\"attrValCode\":\"A000004V000002\",\"attrValName\":\"后付费\"},{\"attrCode\":\"A000005\",\"attrName\":\"号码\",\"attrValCode\":\"18514531295\",\"attrValName\":\"18514531295\",\"eopAttrCode\":\"SerialNumber\",\"eopAttrValCode\":\"18514531295\"},{\"attrCode\":\"A000007\",\"attrName\":\"SIM卡号\"},{\"attrCode\":\"A000011\",\"attrName\":\"首月资费方式\"},{\"attrCode\":\"A000020\",\"attrName\":\"号码来源\",\"attrValCode\":\"A000020V000002\",\"attrValName\":\"分组号码\"},{\"attrCode\":\"A000023\",\"attrName\":\"产品ID\",\"attrValCode\":\"90155946\",\"attrValName\":\"腾讯天王卡\"},{\"attrCode\":\"A000025\",\"attrName\":\"归属地市\",\"attrValCode\":\"110\",\"attrValName\":\"110\",\"eopAttrValCode\":\"110\"},{\"attrCode\":\"A000029\",\"attrName\":\"号码组号\",\"attrValCode\":\"85236889\",\"attrValName\":\"腾讯王卡号码组\"},{\"attrCode\":\"A000030\",\"attrName\":\"是否免预存\",\"attrValCode\":\"A000030V000002\",\"attrValName\":\"否\"},{\"attrCode\":\"A000035\",\"attrName\":\"可选产品包\"},{\"attrCode\":\"A000120\",\"attrName\":\"多缴预存\"},{\"attrCode\":\"A000121\",\"attrName\":\"附加产品\"},{\"attrCode\":\"A000122\",\"attrName\":\"主副卡业务标识\"},{\"attrCode\":\"A000149\",\"attrName\":\"主副卡业务标识\"},{\"attrCode\":\"A000170\",\"attrName\":\"定向套餐类型\",\"attrValCode\":\"A000170V000001\",\"attrValName\":\"腾讯王卡\"},{\"attrCode\":\"A000213\",\"attrName\":\"公司编码\"},{\"attrCode\":\"A000257\",\"attrName\":\"本地订单配送\",\"attrValDesc\":\"订单配送\",\"attrValName\":\"本地订单配送\"},{\"attrCode\":\"A000258\",\"attrName\":\"异地订单配送\",\"attrValDesc\":\"订单配送\",\"attrValName\":\"异地订单配送\"}],\"optProductInfo\":[]},\"orderNetinInfo\":{\"custName\":\"杜亚涛\",\"custType\":\"GRKH\",\"monthLimit\":0,\"niceNumber\":0,\"numExpireTime\":\"2059-12-31 23:59:59\",\"numExpireType\":3,\"numGroupKey\":\"85236889\",\"numPreFee\":0,\"numProcId\":\"999993053115965\",\"numState\":0,\"preNumber\":\"18514531295\",\"proKeyMode\":1,\"psptAddr\":\"河北省藁城市南孟镇杜家庄村东兴街371号\",\"psptExpireTime\":\"2050-01-01 00:00:00\",\"psptNo\":\"130182199109265310\",\"psptShorNumber\":\"265310\",\"psptTypeCode\":\"02\",\"userTag\":1},\"orderNetinSubInfo\":[{\"custName\":\"杜亚涛\",\"custType\":\"GRKH\",\"monthLimit\":0,\"niceNumber\":0,\"numExpireTime\":\"2059-12-31 23:59:59\",\"numExpireType\":3,\"numGroupKey\":\"85236889\",\"numPreFee\":0,\"numProcId\":\"999993053115965\",\"numState\":0,\"preNumber\":\"18514531295\",\"proKeyMode\":1,\"psptAddr\":\"河北省藁城市南孟镇杜家庄村东兴街371号\",\"psptExpireTime\":\"2050-01-01 00:00:00\",\"psptNo\":\"130182199109265310\",\"psptShorNumber\":\"265310\",\"psptTypeCode\":\"02\",\"userTag\":1}],\"orderPostInfo\":{\"lgtsCityCode\":\"110100\",\"lgtsDistrictCode\":\"110101\",\"lgtsProvinceCode\":\"110000\",\"mobilePhone\":\"18518466081\",\"postAddr\":\"测试订单，请勿发货\",\"receiverName\":\"杜亚涛\",\"receiverPsptNo\":\"130182199109265310\",\"receiverPsptType\":\"02\"},\"orderPreInfo\":{\"busChannel\":\"BC01\",\"sysChannel\":\"SS01\"},\"orderProtocolInfo\":[{\"protocolId\":\"18049777\",\"protocolParam\":\"{\\\"conadvanceUpper\\\":\\\"____________\\\",\\\"constartdate\\\":\\\"____________\\\",\\\"conenddate\\\":\\\"____________\\\",\\\"terminalmodel\\\":\\\"____________\\\",\\\"conthawmon\\\":\\\"____________\\\",\\\"producttype\\\":\\\"____________\\\",\\\"number\\\":\\\"18514531295\\\",\\\"numthawmon\\\":\\\"____________\\\",\\\"creditrating\\\":\\\"____________\\\",\\\"monfeelim\\\":\\\"____________\\\",\\\"productname\\\":\\\"____________\\\",\\\"numfstmon\\\":\\\"____________\\\",\\\"cardtype\\\":\\\"____________\\\",\\\"tradeType\\\":\\\"____________\\\",\\\"nummon\\\":\\\"____________\\\",\\\"conadvance\\\":\\\"____________\\\",\\\"nummonlim\\\":\\\"____________\\\",\\\"numadvanceUpper\\\":\\\"____________\\\",\\\"terminalfee\\\":\\\"____________\\\",\\\"custName\\\":\\\"杜亚涛\\\",\\\"lstthawmon\\\":\\\"____________\\\",\\\"conmonth\\\":\\\"____________\\\",\\\"protocolCity\\\":\\\"01\\\",\\\"numenddate\\\":\\\"____________\\\",\\\"psptTypeCode\\\":\\\"130182199109265310\\\",\\\"conmon\\\":\\\"____________\\\",\\\"numstartdate\\\":\\\"____________\\\",\\\"protocolProv\\\":\\\"01\\\",\\\"protocolDate\\\":\\\"2019年11月01日\\\",\\\"psptType\\\":\\\"身份证\\\",\\\"custAddress\\\":\\\"中华人民共和国联通\\\",\\\"creditlimit\\\":\\\"____________\\\",\\\"fstfeetype\\\":\\\"付全月\\\",\\\"numadvance\\\":\\\"____________\\\"}\",\"protocolType\":\"00\"}]}";

}
class SaveThread2 implements Runnable{

    ElasticSearchUtils elasticSearchUtils;

    String index;
    JSONObject jsonObject;

    public SaveThread2(String index,ElasticSearchUtils elasticSearchUtils,JSONObject jsonObject){
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
            while(true){
                IndexResponse indexResponse=elasticSearchUtils.insert(index,jsonObject,null);
                String id=indexResponse.getId();
                getData(id);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public JSONObject getData(String id){
        SearchParam searchParam = new SearchParam();
        searchParam.setPageNo(1);
        searchParam.setPageSize(10);
        Map<String, Object> param = new HashMap<>();
        param.put("_id", id);
        searchParam.setParam(param);
        try {
//            Thread.sleep(1000L);
            SearchResponse searchResponse = elasticSearchUtils.search(index, searchParam);
            SearchHit[] datas=searchResponse.getHits().getHits();
            if(datas==null || datas.length==0){
                System.out.println("数据查询失败:"+id);
                return null;
            }
            SearchHit data=datas[0];
            return JSONObject.parseObject(data.getSourceAsString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}