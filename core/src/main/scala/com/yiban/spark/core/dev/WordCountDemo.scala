package com.yiban.spark.core.dev

import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}

object WordCountDemo extends BaseSparkContext {
  def main(args: Array[String]) {
    val sc : SparkContext = getLocalSparkContext("WordCountDemo_by_local")
//    val rdd1 = sc.textFile("hdfs://master01:9000/data/wordcount.txt")
    val rdd1 = sc.textFile("file:///d:/a.txt")
    val rdd2 = rdd1.flatMap(_.split(" "))
    val rdd3 = rdd2.map((_,1))
    val rdd4 = rdd3.reduceByKey(_+_)
    rdd4.collect().foreach(println)
//    rdd4.collect().foreach{
//      line => logInfo(s"value = ${line}")
//    }
    Thread.sleep(3600*1000)
    sc.stop()
  }
}
