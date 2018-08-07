package com.yiban.spark.streaming.dev.kafka10

import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.HasOffsetRanges
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

    /**
      * 获取offset
      * 对应的数据结构为：
        final class OffsetRange private(
            /** Kafka topic name */
            val topic: String,
            /** Kafka partition id */
            val partition: Int,
            /** inclusive starting offset  当前正在消费的offset */
            val fromOffset: Long,
            /** exclusive ending offset  已经收到的offset，但是还没有消费，也就是说[fromOffset, untilOffset)的数据已经被接收到 */
            val untilOffset: Long)

      1、创建有3个partitons的topic input2
      bin/kafka-topics.sh --create  --zookeeper bdp-dev-3:2181/kafka --topic input2 --partitions 3 --replication-factor 1
      2、发送数据：
      a
      a
      a
      dd
      f
      gg
      3、打印的数据：
      OffsetRange(topic: 'input2', partition: 0, range: [0 -> 0]
      OffsetRange(topic: 'input2', partition: 1, range: [0 -> 0]
      OffsetRange(topic: 'input2', partition: 2, range: [0 -> 0]


      OffsetRange(topic: 'input2', partition: 0, range: [0 -> 0]
      OffsetRange(topic: 'input2', partition: 1, range: [0 -> 0]
      OffsetRange(topic: 'input2', partition: 2, range: [0 -> 3]
      (a,3)


      OffsetRange(topic: 'input2', partition: 0, range: [0 -> 0]
      OffsetRange(topic: 'input2', partition: 1, range: [0 -> 0]
      OffsetRange(topic: 'input2', partition: 2, range: [3 -> 5]
      (dd,1)
      (f,1)


      OffsetRange(topic: 'input2', partition: 0, range: [0 -> 0]
      OffsetRange(topic: 'input2', partition: 1, range: [0 -> 0]
      OffsetRange(topic: 'input2', partition: 2, range: [5 -> 6]
      (gg,1)
      */
    messages.foreachRDD{rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(x => System.out.println(x.toString()))
    }


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
