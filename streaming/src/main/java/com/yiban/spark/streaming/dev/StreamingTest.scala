package com.yiban.spark.streaming.dev

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val ssc = new StreamingContext(conf,Seconds(10))

    val lines = ssc.socketTextStream("localhost",9999)
    lines.checkpoint(Seconds(10))
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map((_,1))
    val wordCounts = pairs.reduceByKey(_+_)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
