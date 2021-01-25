package com.data.auto.landing.table.metadata.matcher

import java.sql.Timestamp
import java.time.Instant

import com.data.auto.landing.table.metadata.store.MetaDataStore
import org.apache.commons.lang3.StringUtils

object MetaDataMatcher {

  val timeCol = "exclusive_time"

  def doSingleMatch(dataMap: Map[String, String], tableColSeq: Seq[(String, Boolean)]): Seq[Any] = {
    val dataGetSeq = tableColSeq.map {
      case (tableColName, nullAble) =>
        if (StringUtils.equalsIgnoreCase(timeCol, tableColName)) {
          (Option(Timestamp.from(Instant.now).asInstanceOf[Any]), true)
        } else {
          (dataMap.find {
            case (dataColName, _) => StringUtils.equalsIgnoreCase(tableColName, dataColName)
          }.map(_._2.asInstanceOf[Any]), nullAble)
        }
    }
    if (dataGetSeq.exists {
      case (option, _) => option.isDefined && !option.get.isInstanceOf[Timestamp]
    }) {
      dataGetSeq.map {
        case (valueOption, nullAble) => valueOption.getOrElse(if (nullAble) null else StringUtils.EMPTY)
      }.seq
    } else {
      Seq.empty
    }
  }

  def doMatch(tableDataSeq: Seq[(String, Map[String, String])],
              metaInfo: MetaDataStore.MetaInfo): Seq[(String, Seq[Seq[Any]])] = tableDataSeq.groupBy(_._1).map {
    case (tableName, oneTableSeq) =>
      val tableColSeq = metaInfo.getOrElse(tableName, List.empty)
      if (tableColSeq.nonEmpty) {
        tableName -> oneTableSeq.map {
          case (_, dataMap) => doSingleMatch(dataMap, tableColSeq)
        }.filter(_.nonEmpty)
      } else {
        StringUtils.EMPTY -> Seq.empty
      }
  }.filter(tuple => StringUtils.isNotBlank(tuple._1) && tuple._2.nonEmpty).toSeq
}
