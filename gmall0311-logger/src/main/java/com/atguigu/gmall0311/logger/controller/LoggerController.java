package com.atguigu.gmall0311.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall0311.common.constant.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;



@Slf4j
@RestController //==@Controller +@ResponseBody
public class LoggerController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;


    @PostMapping("log")
    public String dolog(@RequestParam("logString") String logString){
          // 1  补充时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());
         //  2  写日志 （用于离线采集）
        String logJson = jsonObject.toJSONString();
        log.info(logJson);

         // 3 发送kafka

       if( "startup".equals(jsonObject.getString("type")) ){
           kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP,logJson);
       }else{
           kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT,logJson);
       }

        return "success";
    }



}
