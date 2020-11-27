package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author 王继昌
 * @create 2020-11-25 9:02 
 */
object window {
  def main(args: Array[String]): Unit = {
    /**
     * spark的配置及连接
     */
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDStream")
    //1.初始化Stream socketTextStream
    val context = new StreamingContext(sparkConf, Seconds(3))
    val value: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 9999)
    //计算流程RDD
    value.flatMap(_.split(" ")).window(Seconds(12),Seconds(6)).countByValue().print()
    context.start()
    context.awaitTermination()
  }

}
