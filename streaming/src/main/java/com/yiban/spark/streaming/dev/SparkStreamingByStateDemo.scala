package com.yiban.spark.streaming.dev

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 10000347 on 2017/4/6.
  */
object SparkStreamingByStateDemo {
  val logger = Logger.getLogger(SparkStreamingByStateDemo.getClass.getName)
  logger.setLevel(Level.WARN)
  Logger.getLogger("org").setLevel(Level.ERROR)


  def main(args: Array[String]) {
    val checkpointDirectory = "hdfs://master01:9000/spark/streaming/checkpoint/"
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => {createContext(checkpointDirectory)}
    )
    ssc.start()
    logger.warn("2")
    // Start the computation ssc.awaitTermination()
    ssc.awaitTermination()
    logger.warn("3")
  }

  def createContext(checkpointDirectory: String): StreamingContext = {
    //    System.setProperty("hadoop.home.dir", "D:\\source_code\\hadoop-2.5.0")
    val conf = new SparkConf().setMaster("spark://master01:7077").setAppName("NetworkWordCountByState")
    //    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
    val sc = new SparkContext(conf)
    //    val ssc = new StreamingContext(conf, Seconds(5))
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint(checkpointDirectory)
    // val lines = ssc.textFileStream("/home/usr/temp/") val words = lines.flatMap(_.split(" "))
    val lines = ssc.socketTextStream("master01", 9999)
    //    val lines = ssc.socketTextStream("localhost", 9999)
    val pairs = lines.map(word => (word, 1))

    val addFunc = (currentValues: Seq[Int], prevValueState: Option[Int]) => {
      //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
      val currentCount = currentValues.sum
      // 已累加的值
      val previousCount = prevValueState.getOrElse(0)
      // 返回累加后的结果，是一个Option[Int]类型
      Some(currentCount + previousCount)
    }

    val wordCounts = pairs.updateStateByKey[Int](addFunc)
    logger.warn("1")
    wordCounts.print()
    ssc
  }
}
