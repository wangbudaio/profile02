package com.atguigu.handler

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.app.ADS_log
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream

/**
 * @author 王继昌
 * @create 2020-11-25 16:56 
 */
object LastHourAdCountHandler {

     private val sdf = new SimpleDateFormat("hh:mm")

     def getAdHourMintToCount(filterAdsLogDStream: DStream[ADS_log]): Unit ={
       val value: DStream[(String, (String, Long))] = filterAdsLogDStream.window(Minutes(2)).map(log => {
         val data: String = sdf.format(new Date(log.dt))
         ((log.adid, data), 1L)
       }).reduceByKey(_ + _).map {
         case ((adid, data), x) => (adid, (data, x))
       }
       value.groupByKey().mapValues(iter => iter.toList.sortWith(_._1 < _._1)).print()
     }
}
