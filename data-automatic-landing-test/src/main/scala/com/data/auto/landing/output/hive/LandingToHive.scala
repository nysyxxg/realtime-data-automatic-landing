package com.data.auto.landing.output.hive

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

import com.data.auto.landing.output.LandOutputTrait
import com.data.auto.landing.table.metadata.matcher.MetaDataMatcher
import com.data.auto.landing.table.metadata.store.MetaDataStore
import com.data.auto.landing.table.metadata.store.MetaDataStore.MetaInfo
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.LoggerFactory

import scala.collection.convert.WrapAsScala.collectionAsScalaIterable

class LandingToHive(groupId: String, hiveDbName: String, filterTables: Set[String]) extends LandOutputTrait with Serializable {

  override def getMeta: MetaInfo = {
    val hiveMeta = new HiveMetaStoreClient(new HiveConf)
    val tableNameList = hiveMeta.getAllTables(hiveDbName).toList.filter(tableName => {
      filterTables.toList.contains(tableName)
    })

    //    tableNameList.map {
    //      hiveTableName => hiveTableName -> hiveMeta.getFields(hiveDbName, hiveTableName).map(_.getName).toList
    //    }.toMap
    tableNameList.map {
      hiveTableName =>
        hiveTableName -> hiveMeta.getFields(hiveDbName, hiveTableName).map {
          fieldSchema => (fieldSchema.getName, true)
        }.toList
    }.toMap
  }

  override def write(rddStream: DStream[Seq[(String, Map[String, String])]], spark: SparkSession, filterTables: Set[String]) {

    val metaInfoBC = spark.sparkContext.broadcast(MetaDataStore.getMetaDataInfo)

    rddStream.foreachRDD {
      value =>
        val valueSave = value.flatMap(seq => seq).persist(StorageLevel.MEMORY_AND_DISK_SER)
        val dataExistTableSet = valueSave.map(_._1).distinct.collect.toSet
        metaInfoBC.value.filter {
          case (tableName, _) => dataExistTableSet.contains(tableName)
        }.toSeq.par.foreach {
          case (tableName, colList) =>
            val nodeLog = LoggerFactory.getLogger(getClass)
            val schema = StructType(colList.map {
              case (MetaDataMatcher.timeCol, _) => StructField(MetaDataMatcher.timeCol, TimestampType)
              case (other, _) => StructField(other, StringType)
            })
            val dataFrame = spark.createDataFrame(valueSave.filter(_._1.equalsIgnoreCase(tableName)).map {
              case (_, dataMap) => Row.fromSeq(MetaDataMatcher.doSingleMatch(dataMap, colList))
            }, schema).repartition(1)
            val nowInstant = Instant.now
            val toDayStr = DateTimeFormatter.BASIC_ISO_DATE.format(nowInstant.atZone(ZoneId.systemDefault).toLocalDate)
            val addPartitionSql = s"ALTER TABLE $hiveDbName.$tableName ADD IF NOT EXISTS PARTITION ( ds = '$toDayStr' )"
            nodeLog.info(addPartitionSql)
            spark.sql(addPartitionSql)
            val tmpTableName = s"ods_hive_${tableName}_${nowInstant.getEpochSecond}"
            nodeLog.info(s"tmp table name: $tmpTableName.")
            dataFrame.createOrReplaceTempView(tmpTableName)
            val sqlInsert = s"INSERT INTO TABLE $hiveDbName.$tableName PARTITION ( ds = '$toDayStr' ) SELECT ${colList.mkString(",")} FROM $tmpTableName"
            nodeLog.info(sqlInsert)
            spark.sql(sqlInsert)
            spark.sqlContext.dropTempTable(tmpTableName)
        }
        valueSave.unpersist()
    }
  }

  override def writeRDD(rddData: RDD[Seq[(String, Map[String, String])]], spark: SparkSession, filterTables: Set[String]): Unit = {

    val metaInfoBC = spark.sparkContext.broadcast(MetaDataStore.getMetaDataInfo)
    if (!rddData.isEmpty()) {
      val valueSave = rddData.flatMap(seq => seq).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val dataExistTableSet = valueSave.map(_._1).distinct.collect.toSet
      metaInfoBC.value.filter {
        case (tableName, _) => dataExistTableSet.contains(tableName)
      }.toSeq.par.foreach {
        case (tableName, colList) =>
          val nodeLog = LoggerFactory.getLogger(getClass)
          val schema = StructType(colList.map {
            case (MetaDataMatcher.timeCol, _) => StructField(MetaDataMatcher.timeCol, TimestampType)
            case (other, _) => StructField(other, StringType)
          })
          val dataFrame = spark.createDataFrame(valueSave.filter(_._1.equalsIgnoreCase(tableName)).map {
            case (_, dataMap) => Row.fromSeq(MetaDataMatcher.doSingleMatch(dataMap, colList))
          }, schema).repartition(1)
          val nowInstant = Instant.now
          val toDayStr = DateTimeFormatter.BASIC_ISO_DATE.format(nowInstant.atZone(ZoneId.systemDefault).toLocalDate)
          val addPartitionSql = s"ALTER TABLE $hiveDbName.$tableName ADD IF NOT EXISTS PARTITION ( ds = '$toDayStr' )"
          nodeLog.info(addPartitionSql)
          spark.sql(addPartitionSql)
          val tmpTableName = s"ods_hive_${tableName}_${nowInstant.getEpochSecond}"
          nodeLog.info(s"tmp table name: $tmpTableName.")
          dataFrame.createOrReplaceTempView(tmpTableName)
          val sqlInsert = s"INSERT INTO TABLE $hiveDbName.$tableName PARTITION ( ds = '$toDayStr' ) SELECT ${colList.mkString(",")} FROM $tmpTableName"
          nodeLog.info(sqlInsert)
          spark.sql(sqlInsert)
          spark.sqlContext.dropTempTable(tmpTableName)
      }
      valueSave.unpersist()
    }
  }


  override def writeRDD(rddData: List[Seq[(String, Map[String, String])]], spark: SparkSession, filterTables: Set[String]): Unit ={


  }

  override def writeRDD(rddData:Seq[(String, Map[String, String])], spark: SparkSession, filterTables: Set[String]): Unit ={
    rddData.foreach(line => {
      print(line._1 + "----->" + line._2)
    })
  }

}
