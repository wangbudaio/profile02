package com.atguigu.sparkstreaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @author 王继昌
 * @create 2020-11-24 16:33 
 */
object SparkStreaming04_DirectAuto {
  def main(args: Array[String]): Unit = {
    /**
     * spark的配置及连接
     */
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDStream")
    //1.初始化Stream socketTextStream
    val context = new StreamingContext(sparkConf, Seconds(3))

    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguiguConsumer",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val value: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      context,
      LocationStrategies.PreferBrokers,
      ConsumerStrategies.Subscribe[String, String](
        Set("testTopic"), kafkaParams
      ))
    val value1: DStream[(String, Int)] = value
      .map(x=>x.value()).map((_,1)).reduceByKey(_+_)
    value1.print()
    //计算流程RDD
    context.start()
    context.awaitTermination()
  }
}
