package com.yiban.spark.core.dev

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging

class BaseSparkContext extends Logging {
  def getLocalSparkContext(appName:String): SparkContext ={
    System.setProperty("hadoop.home.dir","D:\\Program Files\\hadoop-2.8.5")
    val sparkConf:SparkConf = new SparkConf().setAppName(appName)
      .setMaster("local[*]")
    val sparkContext:SparkContext = new SparkContext(sparkConf)
    sparkContext
  }

  def getRemoteSparkContext(appName:String) : SparkContext = {
    val sparkConf = new SparkConf().setAppName(appName)
      .setMaster("spark://master01:7077")
      .setJars(List("/root/jars/core.jar"))
    sparkConf.set("spark.executor.memory", "10g")
    sparkConf.set("spark.executor.cores", "8")
    sparkConf.set("spark.total.executor.cores", "8")
    val sparkContext:SparkContext = new SparkContext(sparkConf)
    sparkContext
  }
}
