package com.data.auto.landing.output.mysql

import java.io.File
import java.sql.{DriverManager, ResultSet}
import java.util.Properties

import com.data.auto.landing.common.jdk7.ScalaAutoCloseable.wrapAsScalaAutoCloseable
import com.data.auto.landing.output.LandOutputTrait
import com.data.auto.landing.output.partitioner.DataOutputPartitioner
import com.data.auto.landing.engine.util.KafkaOffsetUtil
import com.data.auto.landing.table.metadata.matcher.MetaDataMatcher
import com.data.auto.landing.table.metadata.store.MetaDataStore
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.slf4j.LoggerFactory

import scala.io.Source

class LandToMySQL(groupId: String, propertiesFile: File) extends LandOutputTrait with Serializable {

  private val log = LoggerFactory.getLogger(getClass)

  private val properties = {
    val propertiesTmp = new Properties
    propertiesTmp.load(Source.fromFile(propertiesFile).reader)
    propertiesTmp
  }

  private val connectInfo = (properties.getProperty("url"), properties.getProperty("username"), properties.getProperty("password"))

  override def getMeta: MetaDataStore.MetaInfo = {

    Class.forName(properties.getProperty("driver"))
    val dbName = Option(StringUtils.trimToNull(StringUtils.substringAfterLast(StringUtils.substringBefore(connectInfo._1, "?"), "/"))).getOrElse("default")
    val resultMap = DriverManager.getConnection(connectInfo._1, connectInfo._2, connectInfo._3).use {
      connection =>
        connection.getMetaData.getTables(null, null, null, null).use {
          resultSet => getTableSeq(resultSet, dbName)
        }.map {
          case (cat, schema, table) =>
            table ->
              connection.getMetaData.getColumns(cat, schema, table, null).use {
                resultSet =>
                  Iterator.continually(resultSet).takeWhile(_.next).map {
                    currentSet => (currentSet.getString("COLUMN_NAME"), "YES".equalsIgnoreCase(currentSet.getString("IS_NULLABLE")))
                  }.toList
              }
        }.toMap
    }
    resultMap.foreach(tuple => log.info(tuple.toString))
    resultMap
  }

  def getTableSeq(resultSet: ResultSet, dbName: String): Seq[(String, String, String)] = {
    if (resultSet.next()) {
      val current = (resultSet.getString("TABLE_CAT"),
        resultSet.getString("TABLE_SCHEM"),
        resultSet.getString("TABLE_NAME"))
      val nextResult = getTableSeq(resultSet, dbName)
      if (StringUtils.equalsAnyIgnoreCase(dbName, current._1, current._2)) {
        nextResult.+:(current)
      } else {
        nextResult
      }
    } else {
      Seq.empty
    }
  }

  override def write(rddStream: DStream[Seq[(String, Map[String, String])]], spark: SparkSession, filterTables: Set[String]) {

    val metaInfoBC = spark.sparkContext.broadcast(MetaDataStore.getMetaDataInfo)

    val connectInfoBC = spark.sparkContext.broadcast(connectInfo)

    val filterTablesBC = spark.sparkContext.broadcast(filterTables)

    val groupIdBC = spark.sparkContext.broadcast(groupId)

    rddStream.foreachRDD { rdd =>
      if (rdd.count() > 0) {
        var bl = true
        try {
          val valueSave = rdd.flatMap(seq => seq).filter {
            case (tableName, _) => filterTablesBC.value.isEmpty || filterTablesBC.value.contains(tableName)
          }.persist(StorageLevel.MEMORY_AND_DISK_SER)
          val dataExistTableSet = valueSave.map(_._1).distinct.collect.toList
          valueSave.repartitionAndSortWithinPartitions(new DataOutputPartitioner(dataExistTableSet)).mapPartitions {
            val ci = connectInfoBC.value
            dataIterator => {
              val nodeLog = LoggerFactory.getLogger(getClass)
              val (tableNameSeq, dataMapSeq) = dataIterator.toSeq.unzip
              val tableNameSet = tableNameSeq.distinct.toSet
              val tableName = if (tableNameSet == null || tableNameSet.size != 1) {
                throw new RuntimeException(s"聚合名错误:${tableNameSet.mkString(", ")}")
              } else {
                tableNameSet.head
              }

              val colList = metaInfoBC.value.getOrElse(tableName, List.empty)

              val insertSql = s"INSERT INTO $tableName (`${colList.mkString("`, `")}`) VALUES (${colList.map(_ => "?").mkString(",")}) ON DUPLICATE KEY UPDATE ${colList.map(colName => s"`$colName` = VALUES(`$colName`)").mkString(", ")}"
              nodeLog.info(insertSql)

              @transient
              var conn = DriverManager.getConnection(ci._1, ci._2, ci._3)
              val resultMap = conn.use {
                _.prepareStatement(insertSql).use {
                  preparedStatement =>
                    dataMapSeq.foreach {
                      dataMap =>
                        val dataSeq = MetaDataMatcher.doSingleMatch(dataMap, colList)
                        if (dataSeq.head != null) {
                          dataSeq.zipWithIndex.foreach {
                            case (str, i) => preparedStatement.setObject(i + 1, str)
                          }
                          preparedStatement.addBatch()
                        }
                    }
                    preparedStatement.executeBatch.groupBy(i => i).mapValues(_.length)
                }
              }

              if (conn != null) {
                nodeLog.info(s"conn连接关闭.......")
                conn.close()
              }
              nodeLog.info(s"写入结果 ${resultMap.toString}.")
              resultMap.toSeq.map {
                case (result) => (tableName, result)
              }.iterator
            }

          }.foreach {
            case (tableName, result) => LoggerFactory.getLogger(getClass).info(s"表【$tableName】有XXXX条写入结果是 $result.")
          }
          valueSave.unpersist()
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
            bl = false
        } finally {

        }
        if (bl) {
          val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          for (o <- offsetRanges) {
            println(s"topic=${o.topic},partition=${o.partition},fromOffset=${o.fromOffset},untilOffset=${o.untilOffset}")
          }
          //手动提交offset,默认提交到Checkpoint中
          //recordDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
          var jdbc = connectInfoBC.value
          KafkaOffsetUtil.saveOffsetRanges(jdbc._1, jdbc._2, jdbc._3, groupIdBC.value, offsetRanges)
        }
      }
    }
  }

  override def writeRDD(rddData: RDD[Seq[(String, Map[String, String])]], spark: SparkSession, filterTables: Set[String]) {

    val metaInfoBC = spark.sparkContext.broadcast(MetaDataStore.getMetaDataInfo)

    val connectInfoBC = spark.sparkContext.broadcast(connectInfo)

    val filterTablesBC = spark.sparkContext.broadcast(filterTables)

    val groupIdBC = spark.sparkContext.broadcast(groupId)

    if (!rddData.isEmpty) {
      try {
        val valueSave = rddData.flatMap(seq => seq).filter {
          case (tableName, _) => filterTablesBC.value.isEmpty || filterTablesBC.value.contains(tableName)
        }.persist(StorageLevel.MEMORY_AND_DISK_SER)

        val dataExistTableSet = valueSave.map(_._1).distinct.collect.toList
        valueSave.repartitionAndSortWithinPartitions(new DataOutputPartitioner(dataExistTableSet)).mapPartitions {
          val ci = connectInfoBC.value
          dataIterator => {
            val nodeLog = LoggerFactory.getLogger(getClass)
            val (tableNameSeq, dataMapSeq) = dataIterator.toSeq.unzip
            val tableNameSet = tableNameSeq.distinct.toSet
            val tableName = if (tableNameSet == null || tableNameSet.size != 1) {
              throw new RuntimeException(s"聚合名错误:${tableNameSet.mkString(", ")}")
            } else {
              tableNameSet.head
            }

            val colList = metaInfoBC.value.getOrElse(tableName, List.empty)

            val insertSql = s"INSERT INTO $tableName (`${colList.mkString("`, `")}`) VALUES (${colList.map(_ => "?").mkString(",")}) ON DUPLICATE KEY UPDATE ${colList.map(colName => s"`$colName` = VALUES(`$colName`)").mkString(", ")}"
            nodeLog.info(insertSql)

            @transient
            var conn = DriverManager.getConnection(ci._1, ci._2, ci._3)
            val resultMap = conn.use {
              _.prepareStatement(insertSql).use {
                preparedStatement =>
                  dataMapSeq.foreach {
                    dataMap =>
                      val dataSeq = MetaDataMatcher.doSingleMatch(dataMap, colList)
                      if (dataSeq.head != null) {
                        dataSeq.zipWithIndex.foreach {
                          case (str, i) => preparedStatement.setObject(i + 1, str)
                        }
                        preparedStatement.addBatch()
                      }
                  }
                  preparedStatement.executeBatch.groupBy(i => i).mapValues(_.length)
              }
            }

            if (conn != null) {
              nodeLog.info(s"conn连接关闭.......")
              conn.close()
            }
            nodeLog.info(s"写入结果 ${resultMap.toString}.")
            resultMap.toSeq.map {
              case (result) => (tableName, result)
            }.iterator
          }

        }.foreach {
          case (tableName, result) => LoggerFactory.getLogger(getClass).info(s"表【$tableName】有XXXX条写入结果是 $result.")
        }
        valueSave.unpersist()
      } catch {
        case ex: Exception =>
          ex.printStackTrace()

      } finally {

      }
    }

  }


  override def writeRDD(rddData: List[Seq[(String, Map[String, String])]], spark: SparkSession, filterTables: Set[String]) {

    val metaInfoBC = spark.sparkContext.broadcast(MetaDataStore.getMetaDataInfo)

    val connectInfoBC = spark.sparkContext.broadcast(connectInfo)

    val filterTablesBC = spark.sparkContext.broadcast(filterTables)

    val groupIdBC = spark.sparkContext.broadcast(groupId)

    if (!rddData.isEmpty) {
      try {
        val valueSave = rddData.flatMap(seq => seq).filter {
          case (tableName, _) => filterTablesBC.value.isEmpty || filterTablesBC.value.contains(tableName)
        }
        val ci = connectInfoBC.value
        val dataExistTableSet = valueSave.map(_._1).distinct

        valueSave.foreach(data => {
          val tableName = data._1
          val tableData = data._2
          //  数据库中元数据表结构
          val colList = metaInfoBC.value.getOrElse(tableName, List.empty)
          val nodeLog = LoggerFactory.getLogger(getClass)
          val insertSql = s"INSERT INTO $tableName (`${colList.mkString("`, `")}`) VALUES (${colList.map(_ => "?").mkString(",")}) ON DUPLICATE KEY UPDATE ${colList.map(colName => s"`$colName` = VALUES(`$colName`)").mkString(", ")}"
          nodeLog.info(insertSql)

          @transient
          var conn = DriverManager.getConnection(ci._1, ci._2, ci._3)
          val resultMap = conn.use {
            _.prepareStatement(insertSql).use {
              preparedStatement =>
                val dataSeq = MetaDataMatcher.doSingleMatch(tableData, colList)
                if (dataSeq.head != null) {
                  dataSeq.zipWithIndex.foreach {
                    case (str, i) => preparedStatement.setObject(i + 1, str)
                  }
                  preparedStatement.addBatch()
                }
                preparedStatement.executeBatch.groupBy(i => i).mapValues(_.length)
            }
          }
          if (conn != null) {
            nodeLog.info(s"conn连接关闭.......")
            conn.close()
          }
          nodeLog.info(s"写入结果 ${resultMap.toString}.")
          resultMap.toSeq.map {
            case (result) => (tableName, result)
          }.iterator

        })

      } catch {
        case ex: Exception =>
          ex.printStackTrace()

      } finally {

      }
    }

  }

  override def writeRDD(rddData: Seq[(String, Map[String, String])], spark: SparkSession, filterTables: Set[String]): Unit = {
    // 按照表名对数据进行分区
    var  tableRDD = rddData.groupBy(_._1)
    tableRDD.foreach(line => {
      println("表名+ " + line._1)
      var data = line._2
      data.map(value => {
        println("key=" + value._1 + "\t  value= " + value._2.toString)
      })
      println("------------------------------------------------------")
    })
  }

  override def writeRDD(rddData: Map[String, String], spark: SparkSession, filterTables: Set[String]): Unit = {
    rddData.map(value => {
      println("key=" + value._1 + "\t  value= " + value._2.toString)
    })
  }
}
