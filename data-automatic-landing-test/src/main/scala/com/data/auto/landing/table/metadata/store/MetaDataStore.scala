package com.data.auto.landing.table.metadata.store

import java.util.concurrent.ConcurrentLinkedDeque

object MetaDataStore {

  /**
   * Map["tableName", Seq["colName"] ]
   */
  type MetaInfo = Map[String, List[(String, Boolean)]]

  private val metaInfoDeque = new ConcurrentLinkedDeque[MetaInfo]

  def setMetaDataInfo(metaInfoInput: MetaInfo) {
    metaInfoDeque.clear()
    metaInfoDeque.addFirst(metaInfoInput)
  }

  def infoExisted: Boolean = !metaInfoDeque.isEmpty

  def getMetaDataInfo: MetaInfo = metaInfoDeque.getFirst
}
