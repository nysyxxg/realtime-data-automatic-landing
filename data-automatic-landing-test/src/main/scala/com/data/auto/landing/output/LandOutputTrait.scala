package com.data.auto.landing.output

import com.data.auto.landing.common.jdk7.ScalaAutoCloseable.wrapAsScalaAutoCloseable
import com.data.auto.landing.table.metadata.store.MetaDataStore
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream

trait LandOutputTrait extends Serializable {

  def getMeta: MetaDataStore.MetaInfo

  def write(rddStream: DStream[Seq[(String, Map[String, String])]], spark: SparkSession, filterTables: Set[String])

  def writeRDD(rddData: RDD[Seq[(String, Map[String, String])]], spark: SparkSession, filterTables: Set[String])

  def writeRDD(rddData:  List[Seq[(String, Map[String, String])]], spark: SparkSession, filterTables: Set[String])

  def writeRDD(rddData:Seq[(String, Map[String, String])], spark: SparkSession, filterTables: Set[String])
}
