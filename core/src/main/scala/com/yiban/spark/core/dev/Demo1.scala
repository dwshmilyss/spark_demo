package com.yiban.spark.core.dev

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Demo1 {

  val logger = Logger.getLogger(Demo1.getClass.getName)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val sc:SparkContext = new SparkContext(new SparkConf().setAppName("demo1").setMaster("local[*]"))
    val data = sc.makeRDD(Array(1,2,3,4))
    println(data.getNumPartitions)
  }
}
