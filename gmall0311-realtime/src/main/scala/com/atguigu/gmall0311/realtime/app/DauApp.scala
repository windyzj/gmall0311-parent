package com.atguigu.gmall0311.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0311.common.constant.GmallConstants
import com.atguigu.gmall0311.realtime.bean.StartupLog
import com.atguigu.gmall0311.realtime.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._


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


    val filteredDstream: DStream[StartupLog] = startupLogDstream.transform { rdd =>
      //  driver
      // 利用清单进行过滤 去重
      println("过滤前："+rdd.count())
      val jedis: Jedis = new Jedis("hadoop1", 6379) //driver 每个执行周期查询redis获得清单 通过广播变量发送到executor中
      val dauKey = "dau:" + new SimpleDateFormat("yyyy-MM-dd").format(new Date());
      val dauSet: util.Set[String] = jedis.smembers(dauKey)
      val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
      jedis.close()
      val filteredRDD: RDD[StartupLog] = rdd.filter { startupLog => //executor
        !dauBC.value.contains(startupLog.mid)
      }
      println("过滤后： "+filteredRDD.count())
      filteredRDD

    }

    //  批次内进行去重 ：  安装key 进行分组 每组取一个
    val groupbyMidDstream: DStream[(String, Iterable[StartupLog])] = filteredDstream.map(startuplog=>(startuplog.mid,startuplog)).groupByKey()
    val realFilteredDStream: DStream[ StartupLog ] = groupbyMidDstream.flatMap { case (mid, startlogItr) =>
      startlogItr.take(1)
    }


    realFilteredDStream.cache()

    // 去重   mid      记录每天访问过的mid 形成一个清单

    // 更新清单 存储到redis
    realFilteredDStream.foreachRDD{rdd=>
        // driver
     // redis   1 数据类型是什么  string 好分片   set 好管理 选用  2 key  dau:2019-08-13  3 value  mid
      rdd.foreachPartition{ startuplogItr=>
        val jedis: Jedis = new Jedis("hadoop1", 6379) //executor
        for ( startuplog<- startuplogItr ) {
          val dauKey = "dau:" +startuplog.logDate
            println(dauKey+"::::"+startuplog.mid)
            jedis.sadd(dauKey,startuplog.mid)
        }
        jedis.close()
      }

    }


    realFilteredDStream.foreachRDD{rdd=>

      rdd.saveToPhoenix("gmall0311_dau",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        new Configuration,Some("hadoop1,hadoop2,hadoop3:2181"))

    }





    println("启动流程")
    ssc.start()

    ssc.awaitTermination()


  }

}
