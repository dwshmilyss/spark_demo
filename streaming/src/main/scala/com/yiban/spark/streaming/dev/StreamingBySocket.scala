package com.yiban.spark.streaming.dev

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by 10000347 on 2015/7/6.
 */
object StreamingBySocket {
  def main(args: Array[String]) {
//    if (args.length < 2) {
//      System.err.println("Usage: NetworkWordCount <hostname> <port>")
//      System.exit(1)
//    }

    val sparkConf = new SparkConf().setAppName("StreamingBySocket").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(10));
    // 获得一个DStream负责连接 监听端口:地址
    val lines = ssc.socketTextStream("localhost", 9999);
    // 对每一行数据执行Split操作
    val words = lines.flatMap(_.split(" "));
    // 统计word的数量
    val pairs = words.map(word => (word, 1));
    val wordCounts = pairs.reduceByKey(_ + _);
    // 输出结果
    wordCounts.print();
    ssc.start();             // 开始
    ssc.awaitTermination();  // 计算完毕退出
  }
}
