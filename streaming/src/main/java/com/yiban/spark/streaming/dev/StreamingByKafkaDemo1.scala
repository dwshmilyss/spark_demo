package com.yiban.spark.streaming.dev

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object StreamingByKafkaDemo1 {
  def main(args: Array[String]): Unit = {
    val batch = 5
    val sparkConf = new SparkConf()
    val ssc = new StreamingContext(sparkConf, Seconds(batch.toInt))
    val kafkaStream = {
      val sparkStreamingConsumerGroup = "spark-streaming-consumer-group"
      val kafkaParams = Map(
        "zookeeper.connect" -> "zookeeper1:2181",
        "group.id" -> "spark-streaming-test",
        "zookeeper.connection.timeout.ms" -> "1000")
      val inputTopic = "input-topic"
      val numPartitionsOfInputTopic = 5
      val streams = (1 to numPartitionsOfInputTopic) map { _ =>
        KafkaUtils.createStream(ssc, kafkaParams, Map(inputTopic -> 1), StorageLevel.MEMORY_ONLY_SER).map(_._2)
      }
      val unifiedStream = ssc.union(streams)
      val sparkProcessingParallelism = 1 // You'd probably pick a higher value than 1 in production.
      unifiedStream.repartition(sparkProcessingParallelism)
    }

    // We use accumulators to track global "counters" across the tasks of our streaming app
    val numInputMessages = ssc.sparkContext.accumulator(0L, "Kafka messages consumed")
    val numOutputMessages = ssc.sparkContext.accumulator(0L, "Kafka messages produced")
    // We use a broadcast variable to share a pool of Kafka producers, which we use to write data from Spark to Kafka.
    val producerPool = {
      val pool = createKafkaProducerPool(kafkaZkCluster.kafka.brokerList, outputTopic.name)
      ssc.sparkContext.broadcast(pool)
    }
    // We also use a broadcast variable for our Avro Injection (Twitter Bijection)
    val converter = ssc.sparkContext.broadcast(SpecificAvroCodecs.toBinary[Tweet])

    // Define the actual data flow of the streaming job
    kafkaStream.map { case bytes =>
      numInputMessages += 1
      // Convert Avro binary data to pojo
      converter.value.invert(bytes) match {
        case Success(tweet) => tweet
        case Failure(e) => // ignore if the conversion failed
      }
    }.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val p = producerPool.value.borrowObject()
        partitionOfRecords.foreach { case tweet: Tweet =>
          // Convert pojo back into Avro binary format
          val bytes = converter.value.apply(tweet)
          // Send the bytes to Kafka
          p.send(bytes)
          numOutputMessages += 1
        }
        producerPool.value.returnObject(p)
      })
    })

    // Run the streaming job
    ssc.start()
    ssc.awaitTermination()
  }
}
