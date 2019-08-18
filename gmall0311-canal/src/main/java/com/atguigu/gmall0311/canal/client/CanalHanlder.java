package com.atguigu.gmall0311.canal.client;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall0311.canal.util.MyKafkaSender;
import com.atguigu.gmall0311.common.constant.GmallConstants;

import java.util.List;
import java.util.Random;


public class CanalHanlder {

    String tableName;   //表名
    CanalEntry.EventType eventType; //时间类型  insert update delete
    List<CanalEntry.RowData> rowDataList; //行级

    public CanalHanlder(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDataList) {
        this.tableName = tableName;
        this.eventType = eventType;
        this.rowDataList = rowDataList;
    }

    public  void handle(){
        if(tableName.equals("order_info")&&eventType.equals(CanalEntry.EventType.INSERT)){
            for (CanalEntry.RowData rowData : rowDataList) {
                sendKafka(  rowData, GmallConstants.KAFKA_TOPIC_ORDER);
            }
        }else if(tableName.equals("order_detail")&&eventType.equals(CanalEntry.EventType.INSERT)){
            for (CanalEntry.RowData rowData : rowDataList) {
                sendKafka(  rowData, GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);
            }
        }else if(tableName.equals("user_info")&&(eventType.equals(CanalEntry.EventType.INSERT)||eventType.equals(CanalEntry.EventType.UPDATE))){
            for (CanalEntry.RowData rowData : rowDataList) {
                sendKafka(  rowData, GmallConstants.KAFKA_TOPIC_USER_INFO);
            }
        }


    }

    /**
     * 发送kafka
     * @param rowData
     * @param topic
     */
    private void sendKafka(CanalEntry.RowData rowData,String topic){
        List<CanalEntry.Column> columnsList = rowData.getAfterColumnsList();
        JSONObject jsonObject = new JSONObject();
        for (CanalEntry.Column column : columnsList) {
            System.out.println(column.getName()+"------>"+column.getValue());
            // 发送数据到对应的topic中
            jsonObject.put(column.getName(),column.getValue());
        }
        String rowJson = jsonObject.toJSONString();

        MyKafkaSender.send(topic,rowJson);
        try {
            Thread.sleep(new Random().nextInt(3)*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


}
