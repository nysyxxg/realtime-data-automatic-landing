package com.data.auto.landing.output.partitioner

import org.apache.spark.Partitioner

class DataOutputPartitioner(tableNameList: List[String]) extends Partitioner {

  private val nameMap = tableNameList.zipWithIndex.toMap

  override def numPartitions: Int = nameMap.size

  override def getPartition(key: Any): Int = {
    val tableName = key.asInstanceOf[String]
    nameMap.getOrElse(tableName, 0)
  }

  override def equals(other: Any): Boolean = other match {
    case r: DataOutputPartitioner => r.nameMap.equals(nameMap)
    case _ => false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    var i = 0
    val nameSeq = nameMap.toIndexedSeq
    while (i < nameSeq.size) {
      result = prime * result + nameSeq(i)._1.hashCode
      i += 1
    }
    prime * result
  }
}
