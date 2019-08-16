package com.atguigu.gmall0311.publisher.service.impl;

import com.atguigu.gmall0311.publisher.bean.OrderHourAmount;
import com.atguigu.gmall0311.publisher.mapper.DauMapper;
import com.atguigu.gmall0311.publisher.mapper.OrderMapper;
import com.atguigu.gmall0311.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl  implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Override
    public Long getDauTotal(String date) {
        return dauMapper.getDauTotal(date);
    }

    @Override
    public Map<String, Long> getDauHourCount(String date) {

        List<Map> dauHourCountList = dauMapper.getDauHourCount(date);
        Map<String,Long> hourMap=new HashMap<>();

        for (Map map : dauHourCountList) {
            hourMap.put((String)map.get("LOGHOUR"),(Long)map.get("CT"));
        }

        return hourMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.getOrderAmount(date);
    }

    @Override
    public Map<String, Double> getOrderHourAmount(String date) {
        //把list集合转换成map
        HashMap<String, Double> hourAmountMap = new HashMap<>();
        List<OrderHourAmount> orderHourAmountList = orderMapper.getOrderHourAmount(date);
        for (OrderHourAmount orderHourAmount : orderHourAmountList) {
            hourAmountMap.put(orderHourAmount.getCreateHour(),orderHourAmount.getSumOrderAmount());
        }

        return hourAmountMap;
    }


}
