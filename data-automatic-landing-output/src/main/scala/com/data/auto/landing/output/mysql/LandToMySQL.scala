package com.data.auto.landing.output.mysql

import java.io.File
import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

import com.data.auto.landing.common.jdk7.ScalaAutoCloseable.wrapAsScalaAutoCloseable
import com.data.auto.landing.output.LandOutputTrait
import com.data.auto.landing.output.partitioner.DataOutputPartitioner
import com.data.auto.landing.schema.metadata.matcher.MetaDataMatcher
import com.data.auto.landing.schema.metadata.store.MetaDataStore
import com.data.auto.landing.util.{DBConnUtil, LRUCacheUtil, PropertiesUtil, SqlUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.slf4j.LoggerFactory

import scala.collection.convert.WrapAsJava
import scala.collection.mutable.ListBuffer
import scala.io.Source

object LandToMySQL {

  // 私有化方法
  private def getLandToMySQL(dataBaseName: String, tableName: String, groupId: String, dbConfigfile: File)
  = new LandToMySQL(dataBaseName, tableName, groupId, dbConfigfile)

  // 公有方法
  def getInstance(dataBaseName: String, tableName: String, groupId: String, dbConfigfile: File): LandToMySQL = {
    var landToMySQL = this.getLandToMySQL(dataBaseName, tableName, groupId, dbConfigfile)
    return landToMySQL
  }
}

class LandToMySQL private(dataBaseName: String, tableName: String, groupId: String, dbConfigfile: File)
  extends LandOutputTrait with Serializable {

  private val log = LoggerFactory.getLogger(getClass)

  private val properties = {
    val propertiesTmp = new Properties
    propertiesTmp.load(Source.fromFile(dbConfigfile).reader)
    propertiesTmp
  }

  private val connectInfo = (properties.getProperty("mysql.driver"), properties.getProperty("mysql.url"),
    properties.getProperty("mysql.username"), properties.getProperty("mysql.password"), properties.getProperty("mysql.dataBaseName"))

  override def createDataBase(createDataBaseSql: String): Unit = this.synchronized {
    var connect: Connection = DBConnUtil.getConnection(connectInfo._1, connectInfo._2, connectInfo._3, connectInfo._4)
    val stat = connect.createStatement
    //创建数据库hello
    stat.executeUpdate(createDataBaseSql)
    stat.close
    DBConnUtil.closeConnection(connect)
  }

  override def executeSql(conn: Connection, createTableSql: String): Unit = this.synchronized {
    val stat = conn.createStatement()
    stat.executeUpdate(createTableSql);
    stat.close
  }

  def getAlterTableSql(dbType: String, tableName: String, fieldListNew: List[String],
                       tableFieldListOld: List[String]): String = this.synchronized {
    val newAddFieldList = fieldListNew.filter(field => !tableFieldListOld.contains(field))
    var alterSql = SqlUtil.getAlterTableSql(dbType, tableName, WrapAsJava.seqAsJavaList(newAddFieldList))
    alterSql
  }


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
          // KafkaOffsetUtil.saveOffsetRanges(jdbc._1, jdbc._2, jdbc._3, groupIdBC.value, offsetRanges)
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
    var tableRDD = rddData.groupBy(_._1)
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

  def writeDataWithSparkAndJdbc(records: Iterable[Map[String, String]], spark: SparkSession,
                                hiveFilterTables: Set[String], dbType: String): Unit = {
    // 替换 为新的 数据库
    val url = connectInfo._1.replace(connectInfo._4, dataBaseName)
    var newConnect: Connection = DBConnUtil.getConnection(connectInfo._1, url, connectInfo._3, connectInfo._4)

    // 开始对一个表的数据进行写入
    log.info("-------------数据写入表：" + tableName + "----数据记录数： " + records.size)
    records.foreach(dataVal => { // 处理每一行数据
      if (!dataVal.isEmpty) {
        println("----------------------------------");
        var sortedMap = dataVal.toList.sortBy(_._1) // 对map的key进行排序

        sortedMap.map(value => {
          println("field = " + value._1 + "\t  data= " + value._2)
        })

        val fieldList = sortedMap.map(_._1)
        //获取字段
        var createTableSql = SqlUtil.getCreateTableSql(dbType, tableName, WrapAsJava.seqAsJavaList(fieldList))
        executeSql(newConnect, createTableSql)


        var columnList = new ListBuffer[StructField]
        fieldList.foreach(field => {
          columnList.+=(StructField(field, StringType, true))
        })
        val schema = StructType {
          columnList
        }

        val dataList = sortedMap.map(_._2)
        var list = new ListBuffer[Row]
        list.+=(Row(dataList.mkString(",")))

        val rdd2 = spark.sparkContext.makeRDD(list)
        // 使用Spark写入
        val dataFrame = spark.createDataFrame(rdd2, schema)
        val prop = new Properties()
        prop.put("user", connectInfo._3)
        prop.put("password", connectInfo._4)
        dataFrame.write.mode(SaveMode.Overwrite).jdbc(url, tableName, prop)
      }
    })
    DBConnUtil.closeConnection(newConnect)

  }


  def writeDataWithJdbc(records: Iterable[Map[String, String]], spark: SparkSession,
                        hiveFilterTables: Set[String], dbType: String,
                        lruCache: LRUCacheUtil[String, String]) = {
    // 替换 为新的 数据库
    val url = connectInfo._2.replace(connectInfo._5, dataBaseName)
    var newConnect: Connection = DBConnUtil.getConnection(connectInfo._1, url, connectInfo._3, connectInfo._4)
    newConnect.setAutoCommit(false)
    // 开始对一个表的数据进行写入
    log.info("-------------数据写入表：" + tableName + "----数据记录数： " + records.size)
    var tableNameKey = dbType + "_" + dataBaseName + "_" + tableName
    var tableFieldListOld = lruCache.get(tableNameKey) //获取字段
    records.foreach(dataVal => { // 处理每一行数据
      if (!dataVal.isEmpty) {
        println("----------------------------------");
        var sortedMap = dataVal.toList.sortBy(_._1) // 对map的key进行排序
        sortedMap.map(value => {
          println("field = " + value._1 + "\t  data= " + value._2)
        })

        val fieldListNew = sortedMap.map(_._1)

        if (tableFieldListOld == null) { // 说明是第一次创建
          var createTableSql = SqlUtil.getCreateTableSql(dbType, tableName, WrapAsJava.seqAsJavaList(fieldListNew))
          executeSql(newConnect, createTableSql)
        } else { // 说明已经表，获取对应的字段列表
          // 找出 新增的字段
          var alterSql = getAlterTableSql(dbType, tableName, fieldListNew, tableFieldListOld.toString.split(",").toList)
          executeSql(newConnect, alterSql)
        }

        // 更新缓存
        lruCache.put(tableNameKey, fieldListNew.mkString(","))

        val dataList = sortedMap.map(_._2) // 获取数据

        val nodeLog = LoggerFactory.getLogger(getClass)
        val insertSql = s"INSERT INTO $tableName (`${fieldListNew.mkString("`, `")}`) VALUES (${fieldListNew.map(_ => "?").mkString(",")}) ON DUPLICATE KEY UPDATE ${fieldListNew.map(colName => s"`$colName` = VALUES(`$colName`)").mkString(", ")}"
        nodeLog.info(insertSql)

        newConnect.prepareStatement(insertSql).use(preparedStatement => {
          dataList.zipWithIndex.foreach {
            case (str, i) =>
              preparedStatement.setObject(i + 1, str)
          }
          preparedStatement.addBatch()
          preparedStatement.executeBatch()
        })
      }
    })
    newConnect.commit
    DBConnUtil.closeConnection(newConnect)
  }

  def writeDataWithSparkSql(records: Iterable[Map[String, String]], spark: SparkSession,
                            hiveFilterTables: Set[String], dbType: String): Unit = {
    // 替换 为新的 数据库
    val url = connectInfo._1.replace(connectInfo._4, dataBaseName)
    // 开始对一个表的数据进行写入
    log.info("-------------数据写入表：" + tableName + "----数据记录数： " + records.size)
    records.foreach(dataVal => { // 处理每一行数据
      if (!dataVal.isEmpty) {
        println("----------------------------------");
        var sortedMap = dataVal.toList.sortBy(_._1) // 对map的key进行排序
        sortedMap.map(value => {
          println("field = " + value._1 + "\t  data= " + value._2)
        })

        val fieldList = sortedMap.map(_._1)
        //获取字段
        var createTableSql = SqlUtil.getCreateTableSql(dbType, tableName, WrapAsJava.seqAsJavaList(fieldList))
        spark.sql(createTableSql)

        var columnList = new ListBuffer[StructField]
        fieldList.foreach(field => {
          columnList.+=(StructField(field, StringType, true))
        })
        val schema = StructType {
          columnList
        }

        val dataList = sortedMap.map(_._2)
        var list = new ListBuffer[Row]
        list.+=(Row(dataList.mkString(",")))

        val rdd2 = spark.sparkContext.makeRDD(list)
        // 使用Spark写入
        val dataFrame = spark.createDataFrame(rdd2, schema)
        val prop = new Properties()
        prop.put("user", connectInfo._3)
        prop.put("password", connectInfo._4)
        dataFrame.write.mode(SaveMode.Overwrite).jdbc(url, tableName, prop)
      }
    })
  }

  override def writeIterable(records: Iterable[Map[String, String]], spark: SparkSession,
                             hiveFilterTables: Set[String], dbType: String,
                             lruCache: LRUCacheUtil[String, String]): Unit = {
    // 方法1
    // writeDataWithSparkAndJdbc(records,spark,hiveFilterTables,dbType)
    // 方法 2：
    writeDataWithJdbc(records, spark, hiveFilterTables, dbType, lruCache)

    // 方法3：
    //writeDataWithSparkSql(records,spark,hiveFilterTables,dbType)

  }

}
