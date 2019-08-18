package com.atguigu.gmall0311.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0311.common.constant.GmallConstants
import com.atguigu.gmall0311.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.gmall0311.realtime.util.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer
import org.json4s.native.Serialization

object SaleApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("sale_app").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val orderRecordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER,ssc)
    val orderDetailRecordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL,ssc)
    val userRecordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER_INFO,ssc)

//     orderRecordDstream.foreachRDD{rdd=>
//       println(rdd.map(_.value()).collect().mkString("\n"))
//     }


    val orderDstream: DStream[OrderInfo] = orderRecordDstream.map { record =>
      val jsonstr: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonstr, classOf[OrderInfo])

      //补充时间字段
      val datetimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = datetimeArr(0)
      val hourStr: String = datetimeArr(1).split(":")(0)
      orderInfo.create_hour = hourStr

      //脱敏
      val tuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
      orderInfo.consignee_tel = tuple._1 + "*******"

   //  println("主表！！！！"+orderInfo)
      orderInfo
    }

    val orderInfoWithKeyDstream: DStream[(String, OrderInfo)] = orderDstream.map(orderInfo=>(orderInfo.id,orderInfo))

    val orderDetailDstream: DStream[OrderDetail] = orderDetailRecordDstream.map { record =>
      val jsonstr: String = record.value()
      val orderDetail: OrderDetail = JSON.parseObject(jsonstr, classOf[OrderDetail])
    // println("从表表！！！！"+orderDetail)
      orderDetail
    }

    val orderDetailWithKeyDstream: DStream[(String, OrderDetail)] = orderDetailDstream.map(orderDetail=>(orderDetail.order_id,orderDetail))


    val fulljoinDstream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithKeyDstream.fullOuterJoin(orderDetailWithKeyDstream)


    val saleDetailDstream: DStream[SaleDetail] = fulljoinDstream.flatMap { case (orderId, (orderInfoOpt, orderDetailOpt)) =>
      // 如果orderinfo 不为none
      // 1 如果 从表也不为none 关联从表
      // 2 把自己写入缓存
      // 3  查询缓存
      val saleDetailList: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()
      val jedis = new Jedis("hadoop1", 6379)
      implicit val formats = org.json4s.DefaultFormats
      if (orderInfoOpt != None) {

        val orderInfo: OrderInfo = orderInfoOpt.get
        //1
       //  println("主表："+orderInfo)
        if (orderDetailOpt != None) {

          val orderDetail: OrderDetail = orderDetailOpt.get
    //    println("从表："+orderDetail)
          val saleDetail = new SaleDetail(orderInfo, orderDetail) //合并为宽表对象
          saleDetailList += saleDetail
        }
        //2   type  :  string     key :   order_info:[order_id]  value order_info_json
        val orderInfokey = "order_info:" + orderInfo.id
        //使用json4s 工具把orderInfo 解析为json
        val orderInfoJson: String = Serialization.write(orderInfo)
        // val orderInfoJson: String = JSON.toJSONString(orderInfo)  fastjson解析case class 不适用
        jedis.setex(orderInfokey, 3600, orderInfoJson)
        //3    订单明细 如何保存到redis中 type :    set   key : order_detail:order_id  value  多个order_detail_json
        val orderDetailKey = "order_detail:" + orderInfo.id
        val orderDetailSet: util.Set[String] = jedis.smembers(orderDetailKey)
        import scala.collection.JavaConversions._
        for (orderDetailjson <- orderDetailSet) {
          val orderDetail: OrderDetail = JSON.parseObject(orderDetailjson, classOf[OrderDetail])
          val saleDetail = new SaleDetail(orderInfo, orderDetail)
          saleDetailList += saleDetail
        }

      } else if (orderDetailOpt != None) {
        //  如果orderInfo 为none  从表不为 none

        // 1 把自己写入缓存
        //  order_detail   type:set  key order_detail:order_id , order_detail_json
        val orderDetail: OrderDetail = orderDetailOpt.get
        val orderDetailJson: String = Serialization.write(orderDetail)
        val orderDetailKey = "order_detail:" + orderDetail.order_id
        jedis.sadd(orderDetailKey, orderDetailJson)
        jedis.expire(orderDetailKey, 3600)

        // 2 查询缓存
        val orderInfoKey = "order_info:" + orderDetail.order_id
        val orderInfoJson: String = jedis.get(orderInfoKey)
        if (orderInfoJson != null && orderDetailJson.size > 0) {
          val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
          val saleDetail = new SaleDetail(orderInfo, orderDetail)
          saleDetailList += saleDetail
        }

      }
      jedis.close()
      saleDetailList


    }

    val fullSaleDetailDStream: DStream[SaleDetail] = saleDetailDstream.mapPartitions { saleDetailItr =>
      val jedis: Jedis = new Jedis("hadoop1", 6379)
      val saleDetailList = new ListBuffer[SaleDetail]
      for (saleDetail <- saleDetailItr) {
      ///  println("saleDetail:"+saleDetail)
        val userInfoJson: String = jedis.get("user_info:"+saleDetail.user_id) //查询缓存

        if (userInfoJson != null) {
          val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
          saleDetail.mergeUserInfo(userInfo); //合并数据
        }
        saleDetailList += saleDetail
      }
      jedis.close()
      saleDetailList.toIterator
    }



    fullSaleDetailDStream.foreachRDD{rdd=>
      rdd.foreachPartition{ saledetailItr=>
        val dataList: List[(String, SaleDetail)] = saledetailItr.map(saledetail=>(saledetail.order_detail_id,saledetail)).toList
        MyEsUtil.indexBulk(GmallConstants.ES_INDEX_SALE,dataList)
      }

    }

    // user新增变化数据写入缓存
    userRecordDstream.map(record=>  JSON.parseObject(record.value(),classOf[UserInfo]) ).foreachRDD{rdd=>
     // 用户信息表的缓存  用uid   type   string         key   user_info:[userid] value userInfoJson

      rdd.foreachPartition{ userInfoItr=>

        val jedis: Jedis = new Jedis("hadoop1", 6379)
        implicit val formats = org.json4s.DefaultFormats
        for ( userInfo<- userInfoItr ) {

          val userInfoJson: String = Serialization.write(userInfo)
          val userInfoKey="user_info:"+userInfo.id
          jedis.set(userInfoKey,userInfoJson)
        }
        jedis.close()

      }




    }



    ssc.start()
    ssc.awaitTermination()
  }



}
