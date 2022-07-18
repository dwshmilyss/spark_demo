package com.yiban.spark.streaming.dev.socket

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by 10000347 on 2015/7/6.
 */
object StreamingBySocketAndStateful {

  val logger = Logger.getLogger("org.apache.spark")
  Logger.getLogger("org").setLevel(Level.ERROR)

  def createContext(checkpointDirectory: String): StreamingContext  = {
    val sparkConf = new SparkConf().setAppName("StreamingBySocketAndStateful").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5));
    //使用updateStateByKey前需要设置checkpoint
    //    ssc.checkpoint("hdfs://jobtracker:9000/spark/checkpoint")
    ssc.checkpoint(checkpointDirectory)
    val addFunc = (currValues: Seq[Int], prevValueState: Option[Int]) => {
      //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
      val currentCount = currValues.sum
      // 已累加的值
      val previousCount = prevValueState.getOrElse(0)
      // 返回累加后的结果，是一个Option[Int]类型
      Some(currentCount + previousCount)
    }
    // 获得一个DStream负责连接 监听端口:地址
    val lines = ssc.socketTextStream("localhost", 9999);
    // 对每一行数据执行Split操作
    val words = lines.flatMap(_.split(" "));
    // 统计word的数量
    val pairs = words.map(word => (word, 1));
    val totalWordCounts = pairs.updateStateByKey[Int](addFunc)

    // 输出结果
    totalWordCounts.print();
    ssc
  }

  def main(args: Array[String]) {
    //第一种写法
    val checkpointDirectory = "./checkpoint"
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => {createContext(checkpointDirectory)}
    )
    // 输出结果
    ssc.start();             // 开始
    ssc.awaitTermination();  // 计算完毕退出
  }
}
