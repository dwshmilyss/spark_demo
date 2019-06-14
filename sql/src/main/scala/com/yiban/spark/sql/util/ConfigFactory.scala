package com.yiban.spark.sql.util

import java.io.FileInputStream
import java.util.Properties

object ConfigFactory {
  def load(): Properties = {
    val properties = new Properties()
    val path = Thread.currentThread().getContextClassLoader.getResource("test.properties").getPath
    properties.load(new FileInputStream(path))
//    val in = ConfigFactory.getClass.getClassLoader.getResourceAsStream("test.properties")
//    properties.load(in)
    properties
  }

  def getInt(properties: Properties,param: String):Int = {
    val value =  properties.getProperty(param)
    return 1
  }

  def main(args: Array[String]): Unit = {
    val conf = load()
    println(conf.getProperty("bbb"))
  }
}
