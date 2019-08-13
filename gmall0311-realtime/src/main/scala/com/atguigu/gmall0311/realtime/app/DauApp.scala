package com.atguigu.gmall0311.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0311.common.constant.GmallConstants
import com.atguigu.gmall0311.realtime.bean.StartupLog
import com.atguigu.gmall0311.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {


  def main(args: Array[String]): Unit = {
      val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")

      val ssc = new StreamingContext(sparkConf,Seconds(5))

      val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)




    //  统计日活
    //  转换一下类型 case class  补充两个日期

    val startupLogDstream: DStream[StartupLog] = inputDstream.map { record =>

      val startupJsonString: String = record.value()
      val startupLog: StartupLog = JSON.parseObject(startupJsonString, classOf[StartupLog])


      val datetimeString: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(startupLog.ts))

      startupLog.logDate = datetimeString.split(" ")(0)
      startupLog.logHour = datetimeString.split(" ")(1)

      startupLog
    }


    // 去重   mid      记录每天访问过的mid 形成一个清单

    // 更新清单 存储到redis
    startupLogDstream.foreachRDD{rdd=>
        // driver
     // redis   1 数据类型是什么  string 好分片   set 好管理 选用  2 key  dau:2019-08-13  3 value  mid
      rdd.foreachPartition{ startuplogItr=>
        val jedis: Jedis = new Jedis("hadoop1", 6379) //executor
        for ( startuplog<- startuplogItr ) {
          val dauKey = "dau:" +startuplog.logDate
          // println(dauKey+"::::"+startuplog.mid)
            jedis.sadd(dauKey,startuplog.mid)
        }
        jedis.close()
      }



    }



    // 利用清单进行过滤 去重


    println("启动流程")
    ssc.start()

    ssc.awaitTermination()


  }

}
