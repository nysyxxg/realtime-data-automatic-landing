package com.data.auto.landing.engine.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import org.slf4j.LoggerFactory

import scala.collection.mutable

object KafkaOffsetUtil {
  private val log = LoggerFactory.getLogger(getClass)

  /**
    * 从数据库读取偏移量
    */
  def getOffsetMap(url: String, userName: String, password: String, groupid: String, topic: String) = {
    val offsetMap = mutable.Map[TopicPartition, Long]()
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    var rs: ResultSet = null
    try {
      connection = DriverManager.getConnection(url, userName, password)
      pstmt = connection.prepareStatement("select topic,partitions,untilOffset from kafka_offset where groupid=? and topic=?")
      pstmt.setString(1, groupid)
      pstmt.setString(2, topic)
      rs = pstmt.executeQuery()

      while (rs.next()) {
        offsetMap += new TopicPartition(rs.getString("topic"), rs.getInt("partitions")) -> rs.getLong("untilOffset")
      }
    } catch {
      case ex: Exception => {
        log.error(ex.getMessage)
        ex.printStackTrace()
      }
    } finally {
      if (rs != null) {
        rs.close()
      }
      if (pstmt != null) {
        pstmt.close()
      }
      if (connection != null) {
        connection.close()
      }
    }
    offsetMap
  }

  /**
    * 将偏移量保存到数据库
    */
  def saveOffsetRanges(url: String, userName: String, password: String, groupid: String, offsetRange: Array[OffsetRange]) = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = DriverManager.getConnection(url, userName, password)
      //replace into表示之前有就替换,没有就插入
      pstmt = connection.prepareStatement("replace into kafka_offset (`topic`, `partitions`, `groupid`, `fromOffset`, `untilOffset`) values(?,?,?,?,?)")
      for (o <- offsetRange) {
        pstmt.setString(1, o.topic)
        pstmt.setInt(2, o.partition)
        pstmt.setString(3, groupid)
        pstmt.setLong(4, o.fromOffset)
        pstmt.setLong(5, o.untilOffset)
        pstmt.executeUpdate()
      }
    } catch {
      case ex: Exception => {
        log.error(ex.getMessage)
        ex.printStackTrace()
      }
    } finally {
      if (pstmt != null) {
        pstmt.close()
      }
      if (connection != null) {
        connection.close()
      }
    }
  }
}
