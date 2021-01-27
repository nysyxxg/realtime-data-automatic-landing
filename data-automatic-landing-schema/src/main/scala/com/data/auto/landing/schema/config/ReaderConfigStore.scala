package com.data.auto.landing.schema.config

import java.util.concurrent.ConcurrentLinkedDeque

import org.apache.commons.lang3.StringUtils

object ReaderConfigStore {

  type DataConfig = Map[String, (String, Seq[String], Seq[String])]

  /**
   * Map["source,dataType", Map["workWeixin,department", (数据字段【data】, (存储表名, Map[提取表的字段, (子表名, 下放字段, 上提字段)])) ] ]
   */
  type ReadConfig = Map[String /* source,dataType */ ,
    Map[String /* "workWeixin,department" */ ,
      (String /* "data,department" */ , (String, DataConfig))]]

  private val readConfigDeque = new ConcurrentLinkedDeque[ReadConfig]

  def setReadConfig(readConfig: ReadConfig, tableNameSet: Iterable[String] = null) {
    readConfigDeque.clear()
    readConfigDeque.addFirst(if (tableNameSet != null) {
      readConfig.mapValues {
        _.mapValues {
          case (dataValue, (dataTableName, dataConfigMap)) =>
            val dataTableNameConvert = if (StringUtils.isBlank(dataTableName)) {
              StringUtils.EMPTY
            } else {
              tableNameSet.find(StringUtils.equalsIgnoreCase(_, dataTableName)).orNull
            }
            (dataValue, (dataTableNameConvert, dataConfigMap.mapValues {
              case (subTableName, dataToSubCol, subToDataCol) => (if (StringUtils.isBlank(subTableName)) {
                StringUtils.EMPTY
              } else {
                tableNameSet.find(StringUtils.equalsIgnoreCase(_, subTableName)).orNull
              }, dataToSubCol, subToDataCol)
            }.filter {
              case (_, (colTableName, _, _)) => colTableName != null
            }))
        }.filter {
          case (_, (_, (dataTableName, _))) => dataTableName != null
        }
      }
    } else {
      readConfig
    })
  }

  def configExisted: Boolean = !readConfigDeque.isEmpty

  def getReadConfig: ReadConfig = readConfigDeque.getFirst
}
