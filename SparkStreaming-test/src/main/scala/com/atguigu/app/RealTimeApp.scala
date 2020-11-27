package com.atguigu.app

import java.util.Properties

import com.atguigu.handler.{BlackListHandler, DateAreaCityAdCountHandler, LastHourAdCountHandler}
import com.atguigu.util.{MyKafkaUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author 王继昌
 * @create 2020-11-25 11:21 
 */

/**
 *广告黑名单业务
 */
object RealTimeApp {
  def main(args: Array[String]): Unit = {
    /**
     * spark的配置及连接
     */
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDStream")
    //1.初始化StreamingContext
    val context = new StreamingContext(sparkConf, Seconds(3))
    //2. 消费kafka数据（读取数据）
    val properties: Properties = PropertiesUtil.load("config.properties")
    val topic: String = properties.getProperty("kafka.topic")
    val value: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic,context)

    //3.将从kafka中的数据转换成封装样例类
    val value1: DStream[ADS_log] = value.map(_.value()).map(
      x => {
        val strings: Array[String] = x.split(" ")
        ADS_log(strings(0).toLong, strings(1), strings(2), strings(3), strings(4))
      }
    )

    //4. 需求一.根据mysql中的黑名单过滤当前数据集
    val value2: DStream[ADS_log] = BlackListHandler.filterByBlackList(value1)
    //5. 需求二.将满足要求的用户写入黑名单
    BlackListHandler.addBlackList(value2)

    /**
     * 广告点击量实时统计
     * 描述：实时统计每天各地区各城市各广告的点击总流量，并将其存入MySQL。
     */

    DateAreaCityAdCountHandler.saveDateAreaCityAdCountToMysql(value2)

    /**
     * 最近一小时广告点击量
     * 说明：实际测试时，为了节省时间，统计的是2分钟内广告点击量
     */
    LastHourAdCountHandler.getAdHourMintToCount(value2)

    //打印黑名单

    //资料，脚本，工具类，jar，难点
    context.start()
    context.awaitTermination()
  }
}

case class ADS_log(dt : Long, area:String, city:String, userid:String,adid:String)
