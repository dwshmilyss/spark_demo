package com.yiban.spark.streaming.dev

import java.util.concurrent.TimeUnit

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by 10000347 on 2017/4/7.
  */
object SparkTest {

//  val logger : Logger = Logger.getLogger(SparkTest.getClass.getName)
//  logger.setLevel(Level.WARN)
//  Logger.getLogger("org").setLevel(Level.ERROR)

  val logger  = LoggerFactory.getLogger("org.apache.spark")

  def main(args: Array[String]) {
    //    System.setProperty("hadoop.home.dir", "D:\\source_code\\hadoop-2.5.0")
    // local
    //    val conf = new SparkConf().setAppName("SparkTest").setMaster("local[*]")
    // cluster
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    //    val lines = sc.textFile("file:///d:/aaa.txt")
    val lines = sc.textFile(args(0)) // "hdfs://master01:9000/a.txt"
    // 对每一行数据执行Split操作
    val res = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect()

    res.foreach(println)
    logger.info("count = " + res.length)
    while (true){
    }
  }
}
