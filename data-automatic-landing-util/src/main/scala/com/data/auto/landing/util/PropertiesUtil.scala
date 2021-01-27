package com.data.auto.landing.util

import java.io.File
import java.util.Properties

import scala.io.Source

object PropertiesUtil {
  def getProperties(propertiesFile:File):Properties ={
    val propertiesTmp = new Properties()
    propertiesTmp.load(Source.fromFile(propertiesFile).reader)
    propertiesTmp
  }
}
