import org.apache.spark.streaming.StreamingContext

/**
 * @author 王继昌
 * @create 2020-11-25 10:07 
 */
object SparkStreaming11_stop {

}
// 监控程序
class MonitorStop(ssc: StreamingContext) extends Runnable{
  override def run(): Unit = {
    //获取Hdfs客户端对象
//    val fs: FileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020"),new ConfigUtil)
  }
}