package com.yiban.spark.streaming.dev.kafka10

import kafka.cluster.Partition
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object DirectKafkaWordCount1 {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, group, topics, batch) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(batch.toInt))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toList
    //    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> brokers,
      "group.id" -> group,
      "auto.commit.interval.ms" -> "1000",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
//      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> "true",
      "session.timeout.ms" -> "30000",
      "partition.assignment.strategy" -> "org.apache.kafka.clients.consumer.RangeAssignor"
    )

    val topicPartition0 : TopicPartition = new TopicPartition(topics,0)
    val topicPartition1 : TopicPartition = new TopicPartition(topics,1)
    val topicPartition2 : TopicPartition = new TopicPartition(topics,2)
    val topicPartition3 : TopicPartition = new TopicPartition(topics,3)
    val topicPartition4 : TopicPartition = new TopicPartition(topics,4)
    val topicPartition5 : TopicPartition = new TopicPartition(topics,5)
    val topicPartition6 : TopicPartition = new TopicPartition(topics,6)
    val topicPartition7 : TopicPartition = new TopicPartition(topics,7)
    val topicPartition8 : TopicPartition = new TopicPartition(topics,8)

    val fromOffsets = Map[TopicPartition,Long](
      topicPartition0 -> 2,
      topicPartition1 -> 2,
      topicPartition2 -> 1,
      topicPartition3 -> 2,
      topicPartition4 -> 2,
      topicPartition5 -> 1,
      topicPartition6 -> 2,
      topicPartition7 -> 2,
      topicPartition8 -> 1
    )

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams,fromOffsets)
    )

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_.value())
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
