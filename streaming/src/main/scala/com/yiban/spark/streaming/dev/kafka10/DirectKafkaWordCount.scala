package com.yiban.spark.streaming.dev.kafka10

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object DirectKafkaWordCount {

  val logger:Logger = Logger.getLogger(DirectKafkaWordCount.getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]) {
//    if (args.length < 2) {
//      System.err.println(
//        s"""
//           |Usage: DirectKafkaWordCount <brokers> <topics>
//           |  <brokers> is a list of one or more Kafka brokers
//           |  <topics> is a list of one or more kafka topics to consume from
//           |
//        """.stripMargin)
//      System.exit(1)
//    }

//    val Array(brokers, group, topics, batch) = args

    val Array(brokers, group, topics, batch) = Array[String]("10.21.3.74:9092,10.21.3.75:9092,10.21.3.76:9092,10.21.3.77:9092","test_8_3_g1","test_8_3","5")

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[*]")
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
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> "true",
      "session.timeout.ms" -> "30000",
      "partition.assignment.strategy" -> "org.apache.kafka.clients.consumer.RangeAssignor"
    )
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

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
