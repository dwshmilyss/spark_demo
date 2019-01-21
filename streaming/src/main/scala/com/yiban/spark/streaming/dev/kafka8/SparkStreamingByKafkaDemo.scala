package com.yiban.spark.streaming.dev.kafka8

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by 10000347 on 2017/4/7.
  * 高阶API消费者
  */
object SparkStreamingByKafkaDemo {
  def main(args: Array[String]) {
    val Array(brokers, group, topics, numThreads, checkpoint, batch, window, slide, numPartitions) = args
    val sparkConf = new SparkConf()
    val ssc = new StreamingContext(sparkConf, Seconds(batch.toInt))
    ssc.checkpoint(checkpoint)

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

//    val kafkaStream = {
//      val kafkaParams = Map(
//        "zookeeper.connect" -> brokers,
//        "group.id" -> group,
//        "zookeeper.connection.timeout.ms" -> "1000")
//      val inputTopic = topics
//      val numPartitionsOfInputTopic = 9
//      val streams = (1 to numPartitionsOfInputTopic) map { _ =>
//        KafkaUtils.createStream(ssc, kafkaParams, Map(inputTopic -> 1), StorageLevel.MEMORY_ONLY_SER).map(_._2)
//      }
//      val unifiedStream = ssc.union(streams)
//      val sparkProcessingParallelism = 1 // You'd probably pick a higher value than 1 in production.
//      unifiedStream.repartition(sparkProcessingParallelism)
//    }

//      kafkaStream.map { case bytes =>
//        numInputMessages += 1
//        // Convert Avro binary data to pojo
//        converter.value.invert(bytes) match {
//          case Success(tweet) => tweet
//          case Failure(e) => // ignore if the conversion failed
//        }
//      }.foreachRDD(rdd => {
//        rdd.foreachPartition(partitionOfRecords => {
//          val p = producerPool.value.borrowObject()
//          partitionOfRecords.foreach { case tweet: Tweet =>
//            // Convert pojo back into Avro binary format
//            val bytes = converter.value.apply(tweet)
//            // Send the bytes to Kafka
//            p.send(bytes)
//            numOutputMessages += 1
//          }
//          producerPool.value.returnObject(p)
//        })
//      })
    val kyro = new Kryo()

    val lines = KafkaUtils.createStream(ssc, brokers, group, topicMap,StorageLevel.MEMORY_AND_DISK_SER).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(window.toInt), Seconds(slide.toInt), numPartitions.toInt)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
