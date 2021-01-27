package com.data.auto.landing.parsing.jsondata

import com.data.auto.landing.util.JsonUtils
import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.kafka.clients.consumer.ConsumerRecord
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

  private val keySplitChar = "##"


  def readRecord(jsonMap: Map[String, Any]): Seq[(String, Map[String, String])] = {
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


  def readRecordV1(value: ConsumerRecord[String, String]): Seq[(String, Map[String, String])] = {
    var list = new ListBuffer[(String, Map[String, String])]()
    try {
      var jsonMap: Map[String, Any] = JsonUtils.toObject(value.value, new TypeReference[Map[String, Any]] {})
      val dbType = jsonMap.getOrElse("dbType", "").asInstanceOf[String]
      val database = jsonMap.getOrElse("database", "").asInstanceOf[String]
      val tableNamesArray = jsonMap.getOrElse("tableName", "").toString.split(",")
      if(dbType.isEmpty || database.isEmpty || tableNamesArray.length==0){
        var map = Map[String, String]()
        map += ("error" -> value.value)
        list.+=(new Tuple2("error", map))
        return  list
      }

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
        newJsonMap += ("uuId" -> uuId.toString)
        newJsonMap += ("msgId" -> msgId.toString)
        newJsonMap += ("dataVer" -> dataVer.toString)
        newJsonMap += ("dataStamp" -> dataStamp.toString)
        list.+=(new Tuple2(dbType + keySplitChar + database + keySplitChar + tableNamesArray(0), newJsonMap))
      } else if (tableNamesArray.size > 1) {
        for (i <- 1 to tableNamesArray.length - 1) {
          var tableName = tableNamesArray(i)
          var tableData = dataMap.get(tableName)
          if (tableData != null && !tableData.isEmpty) { // 说明存在子表
            val tableMapData = tableData.get
            var jsonMapSubTable = tableMapData.asInstanceOf[Map[String, String]]
            jsonMapSubTable += ("uuId" -> uuId.toString)
            jsonMapSubTable += ("msgId" -> msgId.toString)
            jsonMapSubTable += ("dataVer" -> dataVer.toString)
            jsonMapSubTable += ("dataStamp" -> dataStamp.toString)
            list.+=(new Tuple2(dbType + keySplitChar + database + keySplitChar + tableName, jsonMapSubTable))
          }
        }

        var tablelist = tableNamesArray.toList
        var newDataMap = dataMap.filter(line => !tablelist.contains(line._1)).asInstanceOf[Map[String, String]]
        newDataMap += ("uuId" -> uuId.toString)
        newDataMap += ("msgId" -> msgId.toString)
        newDataMap += ("dataVer" -> dataVer.toString)
        newDataMap += ("dataStamp" -> dataStamp.toString)
        list.+=(new Tuple2(dbType + keySplitChar + database + keySplitChar + tableNamesArray(0), newDataMap))
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
