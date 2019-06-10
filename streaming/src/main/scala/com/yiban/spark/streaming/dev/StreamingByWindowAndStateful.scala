package com.yiban.spark.streaming.dev

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by 10000347 on 2015/7/6.
 */
object StreamingByWindowAndStateful {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("StreamingBySocketAndStateful").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(args(0).toInt));

    //使用updateStateByKey前需要设置checkpoint
    ssc.checkpoint("hdfs://jobtracker:9000/spark/checkpoint")
    val addFunc = (currValues: Seq[Int], prevValueState: Option[Int]) => {
      //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
      val currentCount = currValues.sum
      // 已累加的值
      val previousCount = prevValueState.getOrElse(0)
      // 返回累加后的结果，是一个Option[Int]类型
      Some(currentCount + previousCount)
    }
    // 获得一个DStream负责连接 监听端口:地址
    val lines = ssc.socketTextStream(args(1), args(2).toInt);
    // 对每一行数据执行Split操作
    val words = lines.flatMap(_.split(" "));
    // 统计word的数量
    val pairs = words.map(word => (word, 1));
    val totalWordCounts = pairs.updateStateByKey[Int](addFunc)
    //Seconds(args(3).toInt)=15 window时间
    //Seconds(args(4).toInt)=10 滑动窗口时间
    //代表每10秒计算一次前15秒钟的数据  此处计算时间的间隔不再由StreamingContext中的Seconds控制（这个只是DStream的进程时间）
//    val totalWordCounts = pairs.reduceByKeyAndWindow(_+_,_-_,Seconds(args(3).toInt),Seconds(args(4).toInt))
    // 输出结果
    totalWordCounts.print();
    ssc.start();             // 开始
    ssc.awaitTermination();  // 计算完毕退出
  }
}
