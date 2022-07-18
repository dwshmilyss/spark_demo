package com.yiban.spark.streaming.dev.utils

import java.util.Properties

object ConfigUtils {
  def loadProperties(path:String) : Config = {
    val properties = new Properties();
    for (in <- resource.managed(ConfigUtils.getClass.getClassLoader.getResourceAsStream(path))){
      properties.load(in)
    }
    new Config(properties)
  }

  class Config(properties: Properties){
    def getInt(key: String) : Int ={
      return properties.getProperty(key).toInt
    }

    def getString(key: String) : String = {
      return properties.getProperty(key)
    }
  }
}
