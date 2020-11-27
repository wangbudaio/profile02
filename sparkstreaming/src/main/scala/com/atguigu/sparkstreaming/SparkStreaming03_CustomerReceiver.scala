package com.atguigu.sparkstreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.{SparkConf, streaming}

/**
 * @author 王继昌
 * @create 2020-11-24 10:58 
 */
object SparkStreaming03_CustomerReceiver {
  def main(args: Array[String]): Unit = {
    /**
     * spark的配置及连接
     */
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("exec1-02")
    val context = new StreamingContext(sparkConf,streaming.Seconds(5))
    val unit: ReceiverInputDStream[String] = context.receiverStream(new RRR("hadoop102",9999))
    val value: DStream[(String, Int)] = unit.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    value.print()
    context.start()
    context.awaitTermination()
  }
}

class RRR(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
  override def onStart(): Unit = {
    new Thread("Socket Receiver"){
      override def run(): Unit = {
        receiver()
      }
    }.start()
  }

  def receiver(): Unit ={
    val socket = new Socket(host,port)
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream,StandardCharsets.UTF_8))
        var input:String = null
        input = reader.readLine()
    while (!isStopped()&& input != null){
      store(input)
      input = reader.readLine()
    }
    reader.close()
    socket.close()
    restart("restart")
  }
  override def onStop(): Unit = ???
}


