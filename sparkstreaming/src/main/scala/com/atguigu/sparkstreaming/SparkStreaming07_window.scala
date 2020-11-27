package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

/**
 * @author 王继昌
 * @create 2020-11-24 18:31 
 */
object SparkStreaming07_window {
  def main(args: Array[String]): Unit = {
    /**
     * spark的配置及连接
     */
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDStream")
    //1.初始化Stream socketTextStream
    val context = new StreamingContext(sparkConf, Seconds(3))
    val value: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 9999)
    //计算流程RDD
    value.flatMap(_.split(" ")).map((_,1)).window(Seconds(12),Seconds(6)).reduceByKey(_+_).print()
    context.start()
    context.awaitTermination()
  }
}
