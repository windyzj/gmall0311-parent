package com.atguigu.gmall0311.realtime.bean

case class StartupLog(mid:String,
                      uid:String,
                      appid:String,
                      area:String,
                      os:String,
                      ch:String,
                      logType:String,
                      vs:String,
                      var logDate:String,
                      var logHour:String,
                       ts:Long
                     ) {

}

