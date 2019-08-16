package com.atguigu.gmall0311.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0311.common.constant.GmallConstants
import com.atguigu.gmall0311.realtime.bean.{AlertInfo, EventInfo}
import com.atguigu.gmall0311.realtime.util.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlertApp {
  //    1 ) 5分钟内 --> 窗口大小  window      窗口 （窗口大小，滑动步长 ）   窗口大小 数据的统计范围    滑动步长 统计频率
  //    2) 同一设备   groupby  mid
  //    3 ) 用不同账号登录并领取优惠劵     没有 浏览商品
  //    map   变换结构 （预警结构）   经过判断  把是否满足预警条件的mid 打上标签
  //    4) filter  把没有标签的过滤掉
  //    5) 保存到ES
  def main(args: Array[String]): Unit = {

      val sparkConf: SparkConf = new SparkConf().setAppName("alert_app").setMaster("local[*]")

      val ssc =new StreamingContext(sparkConf,Seconds(5))

      val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT,ssc)

    val eventInfoDstream: DStream[EventInfo] = inputDstream.map { record =>
      val eventInfo: EventInfo = JSON.parseObject(record.value(), classOf[EventInfo])
      eventInfo

    }

    eventInfoDstream.cache()
    //    1 ) 5分钟内 --> 窗口大小  window      窗口 （窗口大小，滑动步长 ）   窗口大小 数据的统计范围    滑动步长 统计频率

    val eventWindowDStream: DStream[EventInfo] = eventInfoDstream.window(Seconds(300),Seconds(5))
    //    2) 同一设备   groupby  mid
    val groupbyMidDstream: DStream[(String, Iterable[EventInfo])] = eventWindowDStream.map(eventInfo=>(eventInfo.mid,eventInfo)).groupByKey()
    //    3 ) 用三次及以上不同账号登录并领取优惠劵     没有 浏览商品
    //    map   变换结构 （预警结构）   经过判断  把是否满足预警条件的mid 打上标签
    val checkedDstream: DStream[(Boolean, AlertInfo)] = groupbyMidDstream.map { case (mid, eventInfoItr) =>
      val couponUidSet = new util.HashSet[String]()
      val itemsSet = new util.HashSet[String]()
      val eventList = new util.ArrayList[String]()
      var hasClickItem = false
      breakable(
        for (eventInfo: EventInfo <- eventInfoItr) {
          eventList.add(eventInfo.evid) //收集mid的所有操作事件
          if (eventInfo.evid == "coupon") {
            //点击购物券时 涉及登录账号
            couponUidSet.add(eventInfo.uid)
            itemsSet.add(eventInfo.itemid) //收集领取购物券的商品
          }
          if (eventInfo.evid == "clickItem") { //点击商品
            hasClickItem = true
            break() //如果有点击 直接退出
          }
        }
      )

      //判断 符合预警的条件    1)  点击购物券时 涉及登录账号 >=3   2) events not contain  clickItem

      (couponUidSet.size >= 3 && !hasClickItem, AlertInfo(mid, couponUidSet, itemsSet, eventList, System.currentTimeMillis())) //（是否符合条件 ，日志信息）
    }



      val alterDstream: DStream[AlertInfo] = checkedDstream.filter(_._1).map(_._2)


    // 保存到ES中
    alterDstream.foreachRDD{rdd=>
      rdd.foreachPartition{ alertItr=>
        val list: List[AlertInfo] = alertItr.toList
        //提取主键  // mid + 分钟 组合成主键 同时 也利用主键进行去重
        val alterListWithId: List[(String, AlertInfo)] = list.map(alertInfo=> (alertInfo.mid+"_"+ alertInfo.ts/1000/60   ,alertInfo))

        //批量保存
        MyEsUtil.indexBulk(GmallConstants.ES_INDEX_ALERT,alterListWithId)

      }

    }




    ssc.start()
    ssc.awaitTermination()

  }

}


