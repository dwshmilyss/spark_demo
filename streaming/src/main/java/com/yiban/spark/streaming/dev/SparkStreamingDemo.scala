package com.yiban.spark.streaming.dev

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 10000347 on 2017/4/6.
  */
object SparkStreamingDemo {
  val logger = Logger.getLogger(SparkStreamingDemo.getClass.getName)
  logger.setLevel(Level.WARN)
  Logger.getLogger("org").setLevel(Level.ERROR)


  def main(args: Array[String]) {
//    System.setProperty("hadoop.home.dir", "D:\\source_code\\hadoop-2.5.0")
        val conf = new SparkConf().setMaster("spark://master01:7077").setAppName("NetworkWordCount")
//    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
    val sc = new SparkContext(conf)
    //    val ssc = new StreamingContext(conf, Seconds(5))
    val ssc = new StreamingContext(sc, Seconds(5))
    // val lines = ssc.textFileStream("/home/usr/temp/") val words = lines.flatMap(_.split(" "))
        val lines = ssc.socketTextStream("master01", 9999)
//    val lines = ssc.socketTextStream("localhost", 9999)
    val pairs = lines.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    logger.warn("1")
    wordCounts.print()
    ssc.start()
    logger.warn("2")
    // Start the computation ssc.awaitTermination()
    ssc.awaitTermination()
    logger.warn("3")
    //    ssc.start()
  }
}
