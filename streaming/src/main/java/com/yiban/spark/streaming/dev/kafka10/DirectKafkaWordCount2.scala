package com.yiban.spark.streaming.dev.kafka10

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}
import org.slf4j.{Logger, LoggerFactory}


object DirectKafkaWordCount2 {

  val logger: Logger = LoggerFactory.getLogger("org.apache.spark")

  def main(args: Array[String]) {
    //设置打印日志级别
    org.apache.log4j.Logger.getLogger("org.apache.kafka").setLevel(org.apache.log4j.Level.ERROR)
    org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(org.apache.log4j.Level.ERROR)
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
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "group.id" -> group,
      "auto.commit.interval.ms" -> "1000",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> "false",
      "session.timeout.ms" -> "30000"
//      "partition.assignment.strategy" -> "org.apache.kafka.clients.consumer.RangeAssignor"
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

    val fromOffsets_test1 = Map[TopicPartition,Long](
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

    val fromOffsets_test4 = Map[TopicPartition,Long](
      //分区 -> offset
      topicPartition0 -> 0,
      topicPartition1 -> 0,
      topicPartition2 -> 0,
      topicPartition3 -> 0,
      topicPartition4 -> 0,
      topicPartition5 -> 0,
      topicPartition6 -> 0,
      topicPartition7 -> 0,
      topicPartition8 -> 0
    )

    //    val stream = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      LocationStrategies.PreferConsistent,
//      ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
//    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferBrokers,
      ConsumerStrategies.Assign[String, String](fromOffsets_test4.keys.toList, kafkaParams, fromOffsets_test4)
    )


    //为topic的partition指定executor
//    val hostMap = Map[TopicPartition,String](
//      topicPartition0 -> "10.21.3.75",
//      topicPartition1 -> "10.21.3.75",
//      topicPartition2 -> "10.21.3.75",
//      topicPartition3 -> "10.21.3.75",
//      topicPartition4 -> "10.21.3.75",
//      topicPartition5 -> "10.21.3.75",
//      topicPartition6 -> "10.21.3.75",
//      topicPartition7 -> "10.21.3.75",
//      topicPartition8 -> "10.21.3.75"
//    )

//    val stream = KafkaUtils.createDirectStream[String,String](
//      ssc,
//      LocationStrategies.PreferFixed(hostMap),
//      ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
//    )


    // Get the lines, split them into words, count the words and print
    val lines = stream.map(_.value())
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()


    stream.foreachRDD { rdd =>
      val lines = rdd.map(_.value())
      val words = lines.flatMap(_.split(" "))
      val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      wordCounts.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        logger.warn(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        for ((k,v) <- iter){
          logger.warn(s"${k} : ${v}")
        }
      }
      // some time later, after outputs have completed
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
