package com.atguigu.gmall0311.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {

    public Long getDauTotal(String date);

    public List<Map> getDauHourCount(String date);


}
