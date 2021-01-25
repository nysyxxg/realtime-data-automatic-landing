package com.data.auto.landing.common.util

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object JsonUtils {

  private val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def toJson(any: Any): String = objectMapper.writeValueAsString(any)

  def toObject[T](content: String, typeReference: TypeReference[T]): T = objectMapper.readValue(content, typeReference)


}
