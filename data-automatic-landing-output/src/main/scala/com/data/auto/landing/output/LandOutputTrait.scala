package com.data.auto.landing.output

import java.io.File
import java.sql.Connection

import com.data.auto.landing.schema.metadata.store.MetaDataStore
import com.data.auto.landing.util.LRUCacheUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream

trait LandOutputTrait extends Serializable {

  def executeSql(conn: Connection,createTableSql:String)

  def createDataBase(createDataBaseSql:String)

  def getMeta: MetaDataStore.MetaInfo

  def write(rddStream: DStream[Seq[(String, Map[String, String])]], spark: SparkSession, filterTables: Set[String])

  def writeRDD(rddData: RDD[Seq[(String, Map[String, String])]], spark: SparkSession, filterTables: Set[String])

  def writeRDD(rddData: List[Seq[(String, Map[String, String])]], spark: SparkSession, filterTables: Set[String])

  def writeRDD(rddData: Seq[(String, Map[String, String])], spark: SparkSession, filterTables: Set[String])

  def writeRDD(rddData: Map[String, String], spark: SparkSession, filterTables: Set[String])

  def writeIterable(records: Iterable[Map[String, String]], spark: SparkSession,
               hiveFilterTables: Set[String], dbType: String,lruCache : LRUCacheUtil[String, String])
}
