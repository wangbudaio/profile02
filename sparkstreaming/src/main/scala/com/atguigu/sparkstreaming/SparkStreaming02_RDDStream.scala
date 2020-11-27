package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author 王继昌
 * @create 2020-11-24 10:47 
 */
object SparkStreaming02_RDDStream {
  def main(args: Array[String]): Unit = {
    /**
     * spark的配置及连接
     */
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDStream")
    val context = new StreamingContext(sparkConf,Seconds(4))
    val queue: mutable.Queue[RDD[Int]] = mutable.Queue[RDD[Int]]()
    val unit: InputDStream[Int] = context.queueStream(queue,oneAtATime = false)
    val unit1: DStream[(Int, Int)] = unit.map((_,1)).reduceByKey(_+_)
    unit1.print()
    context.start()
    for (i <- 1 to 5) {
      queue += context.sparkContext.makeRDD(1 to 300 , 10)
      Thread.sleep(2000)
    }
    context.awaitTermination()
  }
}
