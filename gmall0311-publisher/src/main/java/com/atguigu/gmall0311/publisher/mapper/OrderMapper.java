package com.atguigu.gmall0311.publisher.mapper;

import com.atguigu.gmall0311.publisher.bean.OrderHourAmount;

import java.util.List;

public interface OrderMapper {

    public  Double  getOrderAmount(String date);

    public List<OrderHourAmount> getOrderHourAmount(String date);

}
