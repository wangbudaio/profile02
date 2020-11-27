package com.atguigu.handler

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.app.ADS_log
import com.atguigu.util.JDBCUtil
import org.apache.spark.streaming.dstream.DStream

/**
 * @author 王继昌
 * @create 2020-11-25 11:33 
 */
object BlackListHandler {
  private val sdf = new SimpleDateFormat("yyyy-MM-dd")


  def addBlackList(filterAdsLogDSteam:DStream[ADS_log])={
    //统计当前批次中单日每个用户点击每个广告的总次数

    //1.转换和累加
    val value: DStream[((String, String, String), Long)] = filterAdsLogDSteam.map(
      aslog => {
        //将时间戳转换为日期字符串
        val date: String = sdf.format(new Date(aslog.dt))
        //返回值
        ((date, aslog.userid, aslog.adid), 1L)
      }
    ).reduceByKey(_ + _)

    //将点击数据大于30的用户写入黑名单
    value.foreachRDD(
      rdd => {
        //每个分区写一次
        rdd.foreachPartition(
          iter => {
            val connection: Connection = JDBCUtil.getConnection
            iter.foreach{
              case ((dt , userid , adid), count) =>JDBCUtil.executeUpdate(
                connection,
                """
                  |insert into user_ad_count (dt , userid , adid, count)
                  |values (?,?,?,?)
                  |on duplicate key
                  |update count=count+?
                  |""".stripMargin,
                Array(dt , userid , adid,count,count)
              )
                val counts: Long = JDBCUtil.getDataFromMysql(
                  connection,
                  """
                    |select count from user_ad_count where dt=? and userid=? and adid=?
                    |""".stripMargin,
                  Array(dt,userid,adid)
                )
                if (counts >= 30000) {
                  JDBCUtil.executeUpdate(
                    connection,
                    """
                      |insert into black_list (userid)
                      |values (?)
                      |on duplicate key
                      |update userid=?
                      |""".stripMargin,
                    Array(userid, userid)
                  )
                }
            }
            connection.close()
          }
        )
      }
    )
  }




  //判断数据是否在黑名单中，过滤
  def  filterByBlackList (adsLogDStream:DStream[ADS_log])={
    adsLogDStream.filter(
      ads => {
        val connection: Connection = JDBCUtil.getConnection
        val bool: Boolean = JDBCUtil.isExist(
          connection,
          """
            |select * from black_list  where userid=?
            |""".stripMargin,
          Array(ads.userid)
        )
        connection.close()
        !bool
      }
    )

  }

}
