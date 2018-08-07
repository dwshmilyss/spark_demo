package com.yiban.spark.streaming.dev

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
  * Created by 10000347 on 2017/4/7.
  */
object SparkTest {

  //  val logger : Logger = Logger.getLogger(SparkTest.getClass.getName)
  //  logger.setLevel(Level.WARN)
  //  Logger.getLogger("org").setLevel(Level.ERROR)

  val logger = LoggerFactory.getLogger("org.apache.spark")

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkTest").setMaster("spark://master01:7077")
    val ssc = new StreamingContext(conf,Seconds(5))
    //    val lines = sc.textFile("file:///d:/aaa.txt")
    //    val lines = sc.textFile(args(0)) // "hdfs://master01:9000/a.txt"
    //    // 对每一行数据执行Split操作
    val lines = ssc.socketTextStream("10.21.3.73",9999)
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
