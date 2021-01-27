package com.data.auto.landing.output.log

import java.io.File
import java.sql.Connection

import com.data.auto.landing.output.LandOutputTrait
import com.data.auto.landing.schema.metadata.store.MetaDataStore.MetaInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream

class LandToLogInfo(dbConfigfile: File)extends LandOutputTrait with Serializable {
  override def getMeta: MetaInfo = ???

  override def write(rddStream: DStream[Seq[(String, Map[String, String])]], spark: SparkSession, filterTables: Set[String]): Unit = ???

  override def writeRDD(rddData: RDD[Seq[(String, Map[String, String])]], spark: SparkSession, filterTables: Set[String]): Unit = ???

  override def writeRDD(rddData: List[Seq[(String, Map[String, String])]], spark: SparkSession, filterTables: Set[String]): Unit = ???

  override def writeRDD(rddData: Seq[(String, Map[String, String])], spark: SparkSession, filterTables: Set[String]): Unit = {
    rddData.foreach(line => {
      println("表名+ " + line._1)
      var data = line._2
      data.map(value => {
        println("key=" + value._1 + "\t  value= " + value._2)
      })
    })
  }

  override def writeRDD(rddData: Map[String, String], spark: SparkSession, filterTables: Set[String]): Unit = {
    rddData.map(value => {
      println("key=" + value._1 + "\t  value= " + value._2.toString)
    })
  }

  override def createTable(conn: Connection,createTableSql: String): Unit = ???

  override def createDataBase(conn: Connection,createDataBaseSql: String): Unit = ???
}
