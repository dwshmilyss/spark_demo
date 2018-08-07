package com.yiban.spark.streaming.dev

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 以统计词频为例
  * 测试包括
  * 1/ reduceByKey
  * 2/ updateStateByKey
  * 3/ reduceByKeyAndWindow
  */
object StreamingTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("spark://master01:7077").setAppName("test")
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.checkpoint("/spark/checkpoint")
    val lines = ssc.socketTextStream("10.21.3.73",9999)
    lines.checkpoint(Seconds(10))
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map((_,1))

    /**
      *
      * @param currentVals 当前DStream中的数据
      * @param oldValOpt 上一次sum完后的数据
      * @return
      */
    def addFunc(currentVals:Seq[Int],oldValOpt:Option[Int]):Option[Int] = {
      val currSumVal = currentVals.sum
      val oldVal = oldValOpt.getOrElse(0)
      Some(currSumVal + oldVal)
    }

//    val wordCounts = pairs.reduceByKey(_+_)
//    val wordCounts = pairs.updateStateByKey(addFunc _)
    /**
      * reduceByKeyAndWindow(_+_) 每次全量计算前30秒的数据
      */
    //    val wordCounts = pairs.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(30), Seconds(10))
    /**
      * reduceByKeyAndWindow(_+_,_-_) 每次增量计算前30秒的数据，可以复用上一次计算的结果，所以会有重复的时间批次，如果是window是30秒 滑动是10秒
      * 那么有20秒的数据是重复的
      * 所以第一个函数就是上一个30秒的数据 + 增量10秒的数据（上一次window和这一次window的差值）
      * 第二个函数就是上一个30秒的数据 - 上一个30秒里面前10秒的数据
      */
    val wordCounts = pairs.reduceByKeyAndWindow((a:Int,b:Int) => (a + b),(a:Int,b:Int) => (a - b), Seconds(30), Seconds(10))
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}