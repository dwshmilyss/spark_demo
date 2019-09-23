package com.yiban.spark.streaming.dev.socket

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by 10000347 on 2015/7/6.
 */
object StreamingBySocket {

  val logger = Logger.getLogger("org.apache.spark")
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkTest").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))
    //    val lines = sc.textFile("file:///d:/aaa.txt")
    //    val lines = sc.textFile(args(0)) // "hdfs://master01:9000/a.txt"
    //    // 对每一行数据执行Split操作
    val lines = ssc.socketTextStream("localhost",9999)
    val res = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //
    //    res.foreach(println)
    //    logger.info("count = " + res.length)
    //    while (true){
    //    }
    res.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
