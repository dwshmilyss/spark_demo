package com.yiban.spark.streaming.dev

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by 10000347 on 2017/4/7.
  */
object SparkStreamingByKafkaDemo {
  def main(args: Array[String]) {
    val Array(zkQuorum, group, topics, numThreads, checkpoint, batch, window, slide, numPartitions) = args
    val sparkConf = new SparkConf()
    val ssc = new StreamingContext(sparkConf, Seconds(batch.toInt))
    ssc.checkpoint(checkpoint)

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(window.toInt), Seconds(slide.toInt), numPartitions.toInt)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
