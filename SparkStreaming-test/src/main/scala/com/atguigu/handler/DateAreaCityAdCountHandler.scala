package com.atguigu.handler

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.app.ADS_log
import com.atguigu.util.JDBCUtil
import org.apache.spark.streaming.dstream.DStream

/**
 * @author 王继昌
 * @create 2020-11-25 15:04
 */
object DateAreaCityAdCountHandler {

  private val dfs = new SimpleDateFormat("yyyy-MM-dd")

  def saveDateAreaCityAdCountToMysql(filterAdsLogDStream: DStream[ADS_log]): Unit ={
    val value: DStream[((String, String, String, String), Long)] = filterAdsLogDStream.map(
      log => {
        val data: String = dfs.format(new Date(log.dt))
        ((log.area, data, log.city, log.adid), 1L)
      }
    ).reduceByKey(_ + _)

    value.foreachRDD(
        rdd=> rdd.foreachPartition {
          iter => {
              val connection: Connection = JDBCUtil.getConnection
              iter.foreach{
                case ((area, data, city, adid),count) => {
                  JDBCUtil.executeUpdate(
                    connection,
                    """
                      |insert into area_city_ad_count (dt,area,city,adid,count)
                      |values (?,?,?,?,?)
                      |ON DUPLICATE KEY
                      |UPDATE count=count+?;
                      |""".stripMargin,
                    Array(area, data, city, adid,count,count)
                  )
                  }
              }
              //c.释放连接
              connection.close()
            }
          }
    )

  }
}
