package com.yiban.spark.streaming.dev.utils

import java.util.Properties

import resource.managed

object ConfigFactory {
  def load(): Config = {
    val properties = new Properties()
//    val path = Thread.currentThread().getContextClassLoader.getResource("test.properties").getPath
//    properties.load(new FileInputStream(path))
//    val in = ConfigFactory.getClass.getClassLoader.getResourceAsStream("test.properties")
    for(in <- managed(ConfigFactory.getClass.getClassLoader.getResourceAsStream("test.properties"))){
      properties.load(in)
    }
    new Config(properties)
  }

  def main(args: Array[String]): Unit = {
    val conf = load()
    println(conf.getString("aaa"))
  }

  class Config(properties: Properties){
    def getInt(param:String):Int = {
      return properties.getProperty(param).toInt
    }

    def getString(param:String):String = {
      return properties.getProperty(param)
    }
  }
}
