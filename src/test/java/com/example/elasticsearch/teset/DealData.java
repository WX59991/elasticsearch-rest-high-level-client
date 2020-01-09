package com.example.elasticsearch.teset;

import com.alibaba.fastjson.JSONObject;
import com.example.elasticsearch.https.HttpClientUtil;
import org.springframework.util.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author wangxia
 * @date 2019/11/4 10:54
 * @Description:
 */
public class DealData {


    public static List<String> readDataFromFile(String filename){
        List<String> datas=new ArrayList<>();
        try(BufferedReader reader=new BufferedReader(new InputStreamReader(new FileInputStream(filename)))){
            String temp=null;
            while((temp=reader.readLine())!=null){
                datas.add(temp);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return datas;
    }

    public static void writeDataFromFile(String filename,List<String> data,List<String> result){
        try(BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename)))){
            for(int i=0;i<data.size();i++){
                StringBuilder temp=new StringBuilder();
                temp.append(data.get(i));
                temp.append("\t");
                temp.append(result.get(i));
                temp.append("\r\n");
                writer.write(temp.toString());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static String getResponse(String data) throws  Exception{
        String url="http://10.191.129.51:30000/ansj/getAnsjResult";
        Map<String,String> datas=new HashMap<>();
        datas.put("data",data);
        String response=HttpClientUtil.doPost(url,datas);
        String res="";
        if(!StringUtils.isEmpty(response)){
            Object dataarr=JSONObject.parseObject(response).get("data");
            res=dataarr==null?"":dataarr.toString();
        }
        return res;
    }

    public static void main(String[] args){
        List<String> datas=readDataFromFile("C:\\work\\data.txt");
        List<String> result=new ArrayList<>();
        for(String data:datas){
            String  tt="";
            try{
                tt=getResponse(data);
            }catch (Exception e){ }
            result.add(tt);
        }
        writeDataFromFile("C:\\work\\data2.txt",datas,result);
    }

}
