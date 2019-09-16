package com.yiban.spark.streaming.dev

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by 10000347 on 2015/7/7.
 */
object StreamingByLocalFile {
  val logger = Logger.getLogger(StreamingByLocalFile.getClass.getName)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]) {
    val sc = new SparkConf().setAppName("StreamingByLocalFile").setMaster("local[2]")
    val ssc = new StreamingContext(sc,Seconds(5))
    val lines = ssc.textFileStream("file:///d:/test")
    // 对每一行数据执行Split操作
    val words = lines.flatMap(_.split(" "));
    // 统计word的数量
    val pairs = words.map(word => (word, 1));
    val wordCounts = pairs.reduceByKey(_ + _);
    logger.info("data = "+wordCounts)
    // 输出结果
    wordCounts.print();
    ssc.start();             // 开始
    ssc.awaitTermination();  // 计算完毕退出
  }
}
