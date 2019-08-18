package com.atguigu.gmall0311.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0311.publisher.bean.Option;
import com.atguigu.gmall0311.publisher.bean.Stat;
import com.atguigu.gmall0311.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;


    @GetMapping("realtime-total")
    public  String getTotal(@RequestParam("date") String date){
        Long dauTotal = publisherService.getDauTotal(date);
        List<Map>  totalList=new ArrayList<>();
        Map dauMap= new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);
        totalList.add(dauMap);


        Map newMidMap= new HashMap();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",33333);
        totalList.add(newMidMap);

        Map orderAmountMap= new HashMap();
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        Double orderAmount = publisherService.getOrderAmount(date);
        orderAmountMap.put("value",orderAmount);
        totalList.add(orderAmountMap);

         return  JSON.toJSONString(totalList);

    }

    @GetMapping("realtime-hour")
    public  String getRealtimeHour(@RequestParam("id") String id,@RequestParam("date")String tdate){
        if("dau".equals(id)){
            Map<String, Long> dauHourCountTodayMap = publisherService.getDauHourCount(tdate);

            String ydate = getYesterdayString(tdate);
            Map<String, Long> dauHourCountYDayMap = publisherService.getDauHourCount(ydate);

            Map dauMap=new HashMap();

            dauMap.put("today",dauHourCountTodayMap);
            dauMap.put("yesterday",dauHourCountYDayMap);

            return JSON.toJSONString(dauMap);
        }else if("order_amount".equals(id)){
            Map<String, Double> orderHourAmountTodayMap = publisherService.getOrderHourAmount(tdate);

            String ydate = getYesterdayString(tdate);
            Map<String, Double> orderHourAmountYdayMap = publisherService.getOrderHourAmount(ydate);

            Map orderAmountMap=new HashMap();

            orderAmountMap.put("today",orderHourAmountTodayMap);
            orderAmountMap.put("yesterday",orderHourAmountYdayMap);

            return JSON.toJSONString(orderAmountMap);
        }

        return null;


    }



    private String getYesterdayString(String  todayString){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yesterdayString=null;
        try {
            Date today = dateFormat.parse(todayString);
            Date yesterday = DateUtils.addDays(today, -1);
              yesterdayString = dateFormat.format(yesterday);

        } catch (ParseException e) {
            e.printStackTrace();
        }
        return  yesterdayString;

    }


    @GetMapping("sale_detail")
    public  String getSaleDetail(@RequestParam("date") String date,@RequestParam("keyword")String keyword,@RequestParam("startpage")int startpage,@RequestParam("size")int size ){
            //根据参数查询es

        Map<String, Object> saleDetailMap = publisherService.getSaleDetailFromES(date, keyword, startpage, size);
        Long total =(Long)saleDetailMap.get("total");
        List saleList =(List) saleDetailMap.get("saleList");
        Map genderMap =(Map) saleDetailMap.get("genderMap");
        Map ageMap = (Map)saleDetailMap.get("ageMap");


        Long maleCount =(Long)genderMap.get("M");
        Long femaleCount =(Long)genderMap.get("F");

        Double maleRatio= Math.round(maleCount*1000D/total)/10D;
        Double femaleRatio= Math.round(femaleCount*1000D/total)/10D;

        List genderOptionList=new ArrayList();
        genderOptionList.add( new Option("男",maleRatio));
        genderOptionList.add( new Option("女",femaleRatio));

        Stat genderStat = new Stat("性别占比", genderOptionList);


        Long age_20count=0L;
        Long age20_30count=0L;
        Long age30_count=0L;
        for (Object o : ageMap.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            String ageString = (String)entry.getKey();
            Long ageCount = (Long)entry.getValue();
            if(Integer.parseInt(ageString)<20){
                age_20count+=ageCount;
            }else if(Integer.parseInt(ageString)>=20 &&Integer.parseInt(ageString)<=30){
                age20_30count+=ageCount;
            }else{
                age30_count+=ageCount;
            }
        }

        Double age_20Ratio= Math.round(age_20count*1000D/total)/10D;
        Double age20_30Ratio= Math.round(age20_30count*1000D/total)/10D;
        Double age30_Ratio= Math.round(age30_count*1000D/total)/10D;

        List ageOptionList=new ArrayList();
        ageOptionList.add( new Option("20岁以下",age_20Ratio));
        ageOptionList.add( new Option("20岁到30岁",age20_30Ratio));
        ageOptionList.add( new Option("30岁以上",age30_Ratio));

        Stat ageStat = new Stat("年龄段占比", ageOptionList);

        List statList=new ArrayList();
        statList.add(genderStat);
        statList.add(ageStat);


        Map finalResultMap=new HashMap();
        finalResultMap.put("total",total);
        finalResultMap.put("stat",statList);
        finalResultMap.put("detail" ,saleList);


         return JSON.toJSONString(finalResultMap);
    }




}
