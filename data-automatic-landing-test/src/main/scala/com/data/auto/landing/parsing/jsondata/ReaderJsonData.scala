package com.data.auto.landing.parsing.jsondata

import com.data.auto.landing.common.util.JsonUtils
import com.data.auto.landing.table.config.ReaderConfigStore
import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
  * 支持读取Json数据格式
  */
object ReaderJsonData {

  private val log = LoggerFactory.getLogger(getClass)

  private val levelSplitChar = "/"

  private val sameSplitChar = ","

  private val constantSplitChar = ":"


  def readRecord(jsonMap:Map[String, Any]): Seq[(String, Map[String, String])] = {
    var list = new ListBuffer[(String, Map[String, String])]()
    try {
      var database = jsonMap.getOrElse("database", "")
      val tableNamesArray = jsonMap.getOrElse("tableName", "").toString.split(",")
      var uuId = jsonMap.getOrElse("uuId", "")
      var msgId = jsonMap.getOrElse("msgId", "")
      var dataVer = jsonMap.getOrElse("dataVer", "")
      var dataStamp = jsonMap.getOrElse("dataStamp", "")
      var dataMap = jsonMap.get("data").get.asInstanceOf[Map[String, Any]]
      if (tableNamesArray.size == 1) {
        var newJsonMap: Map[String, String] = dataMap.map(line => {
          var key = line._1
          var value = line._2
          (key, value.toString)
        })
        newJsonMap += ("database" -> database.toString)
        newJsonMap += ("uuId" -> uuId.toString)
        newJsonMap += ("msgId" -> msgId.toString)
        newJsonMap += ("dataVer" -> dataVer.toString)
        newJsonMap += ("dataStamp" -> dataStamp.toString)
        list.+=(new Tuple2(tableNamesArray(0), newJsonMap))
      } else if (tableNamesArray.size > 1) {
        for (i <- 1 to tableNamesArray.length - 1) {
          var tableName = tableNamesArray(i)
          var tableData = dataMap.get(tableName)
          if (tableData != null && !tableData.isEmpty) { // 说明存在子表
            val tableMapData = tableData.get
            var jsonMapSubTable = tableMapData.asInstanceOf[Map[String, String]]
            jsonMapSubTable += ("database" -> database.toString)
            jsonMapSubTable += ("uuId" -> uuId.toString)
            jsonMapSubTable += ("msgId" -> msgId.toString)
            jsonMapSubTable += ("dataVer" -> dataVer.toString)
            jsonMapSubTable += ("dataStamp" -> dataStamp.toString)
            list.+=(new Tuple2(tableName, jsonMapSubTable))
          }
        }

        var tablelist = tableNamesArray.toList
        var newDataMap = dataMap.filter(line => !tablelist.contains(line._1)).asInstanceOf[Map[String, String]]
        newDataMap += ("database" -> database.toString)
        newDataMap += ("uuId" -> uuId.toString)
        newDataMap += ("msgId" -> msgId.toString)
        newDataMap += ("dataVer" -> dataVer.toString)
        newDataMap += ("dataStamp" -> dataStamp.toString)
        list.+=(new Tuple2(tableNamesArray(0), newDataMap))
      }
      list
    } catch {
      case e: Exception =>
        log.error("不是json格式！", e)
        list
    }
    list
  }


  /**
    * createKeyMapIteratorAndParentConfigMap
    */
  private def createKeyMapIteratorAndParentConfigMap(parentConfigMapOrigin: ReaderConfigStore.DataConfig,
                                                     dataKeyStr: String): (Iterator[(String, Seq[String])], ReaderConfigStore.DataConfig) = {
    val dataKeySeq = StringUtils.split(dataKeyStr, levelSplitChar).toList
    val dataKeySize = dataKeySeq.size
    val parentConfigMap = parentConfigMapOrigin.toList.map {
      case (colName, keyTableTuple) =>
        val colSeq = StringUtils.split(colName, levelSplitChar).toList
        (colSeq.count(_.equals("..")), colSeq.last) -> keyTableTuple
    }.groupBy(_._1._1).mapValues {
      _.map {
        case ((_, colName), keyTableTuple) => (colName, keyTableTuple)
      }
    }
    (dataKeySeq.zipWithIndex.map {
      case (dataKey, keyIndex) => (dataKey, parentConfigMap.getOrElse(dataKeySize - keyIndex, List.empty).map(_._1))
    }.iterator, parentConfigMap.flatMap(_._2).filter {
      case (_, (subTableName, _, _)) => StringUtils.isNotBlank(subTableName)
    })
  }

  /**
    * readDataMap
    */
  private def readDataMap(dataMap: Map[String, Any],
                          dataHiveTable: String,
                          dataConfigMap: ReaderConfigStore.DataConfig): Seq[(String, Map[String, String])] = {
    if (dataConfigMap != null && dataConfigMap.nonEmpty) {
      val (subTableData, normalData) = dataMap.map {
        case (dataColName, dataValue) => (dataColName, dataValue, dataConfigMap.get(dataColName).orNull)
      }.partition(_._3 != null)
      val (subTableDataValue, subTableDataSeq) = subTableData.map {
        case (dataColName, dataValue, (subTableName, dataToSubCol, subToDataCol)) =>
          val (toSubConstant, dataToSubKeySeq) = dataToSubCol.partition(_.contains(constantSplitChar))
          val dataToSubMap = dataMap.filterKeys(dataToSubKeySeq.contains).++(toSubConstant.map {
            constantStr =>
              val constantSplit = StringUtils.split(constantStr, constantSplitChar, 2)
              (constantSplit.head, constantSplit.last.asInstanceOf[Any])
          }.toMap)
          val (subToDataMapSeq, subTableMapSeq) = if (subToDataCol.isEmpty) {
            (Seq.empty, dataValue match {
              case dataValueSeq: Seq[_] =>
                dataValueSeq.map {
                  value => (subTableName, value.asInstanceOf[Map[String, Any]].++(dataToSubMap))
                }
              case dataValueMap: Map[_, _] =>
                Seq((subTableName, dataValueMap.asInstanceOf[Map[String, Any]].++(dataToSubMap)))
            })
          } else {
            val (subToDataMapSeq2, subTableMapSeq2) = getSubSeq(subToDataCol, dataValue).unzip
            (subToDataMapSeq2, subTableMapSeq2.map(subTableMap => (subTableName, subTableMap.++(dataToSubMap))))
          }
          ((if (subToDataCol.isEmpty) null.asInstanceOf[String] else dataColName, if (subToDataMapSeq.isEmpty) {
            null
          } else if (subToDataCol.size == 1) {
            subToDataMapSeq.map(_.head._2)
          } else {
            subToDataMapSeq
          }), subTableMapSeq)
      }.unzip
      val subTableDataResult = subTableDataSeq.flatMap(_.seq).toSeq
      (if (StringUtils.isBlank(dataHiveTable)) {
        subTableDataResult
      } else {
        subTableDataResult.+:(dataHiveTable -> normalData.map(tuple => tuple._1 -> tuple._2).++(subTableDataValue.filter {
          tuple => tuple != null && tuple._1 != null
        }).toMap)
      }).map {
        case (tableName, tableDataMap) => tableName -> mapConvert(tableDataMap)
      }
    } else {
      if (StringUtils.isBlank(dataHiveTable)) {
        Seq.empty
      } else {
        Seq(dataHiveTable -> mapConvert(dataMap))
      }
    }
  }

  /**
    * getSubSeq
    *
    * @param keyColNameList Key列名List
    * @param subColData     读取的数据
    * @return Seq[(Map[key列, 数据], subSeq数据)]
    */
  private def getSubSeq(keyColNameList: Seq[String], subColData: Any): Seq[(Map[String, Any], Map[String, Any])] = {
    if (subColData == null) {
      Seq.empty
    } else {
      subColData match {
        case seqData: Seq[_] => seqData.flatMap(getSubSeq(keyColNameList, _))
        case mapData: Map[_, _] =>
          val mapStringAnyData = mapData.asInstanceOf[Map[String, Any]]
          val keyColGet = mapStringAnyData.filterKeys(keyColNameList.contains)
          if (keyColGet.nonEmpty) {
            Seq((keyColGet, mapStringAnyData))
          } else if (mapStringAnyData.size == 1) {
            getSubSeq(keyColNameList, mapStringAnyData.head._2)
          } else {
            throw new RuntimeException(s"不支持: ${JsonUtils.toJson(mapStringAnyData)}")
          }
        case other =>
          throw new RuntimeException(s"不支持: ${JsonUtils.toJson(other)}")
      }
    }
  }

  /**
    * mapConvert
    *
    * @param dataMap 数据Map
    */
  private def mapConvert(dataMap: Map[String, Any]): Map[String, String] = dataMap.mapValues {
    value =>
      if (value == null) {
        null.asInstanceOf[String]
      } else {
        value match {
          case seqValue: Seq[_] =>
            if (seqValue.isEmpty) {
              null.asInstanceOf[String]
            } else {
              seqValue.head match {
                case _: Map[_, _] => JsonUtils.toJson(seqValue)
                case _ => seqValue.map(String.valueOf).mkString(sameSplitChar)
              }
            }
          case mapValue: Map[_, _] =>
            if (mapValue.isEmpty) {
              null.asInstanceOf[String]
            } else {
              JsonUtils.toJson(mapValue)
            }
          case other => String.valueOf(other)
        }
      }
  }
}
