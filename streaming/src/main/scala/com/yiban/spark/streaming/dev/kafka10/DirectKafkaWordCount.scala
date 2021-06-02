package com.yiban.spark.streaming.dev.kafka10

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object DirectKafkaWordCount {

  val logger: Logger = Logger.getLogger(DirectKafkaWordCount.getClass)
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

    val Array(brokers, group, batch) = Array[String]("localhost:9092", "g2", "10")

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(batch.toInt))

    // Create direct kafka stream with brokers and topics
    val topics = Array("t2","t1")
    topics.foreach(println)
    //    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> brokers,
      "group.id" -> group,
//      "auto.commit.interval.ms" -> "1000",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> "false",
      "session.timeout.ms" -> "30000"
      //      "partition.assignment.strategy" -> "org.apache.kafka.clients.consumer.RangeAssignor"
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    // Get the lines, split them into words, count the words and print
    //    val lines = messages.map(_.value())
    //    val words = lines.flatMap(_.split(" "))
    //    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    //    wordCounts.print()

    //    messages.foreachRDD(rdd => {
    //      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //      rdd.foreachPartition { iter =>
    //        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
    //        logger.error(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
    //      }
    //    })

    stream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition(
          partition => {
            val o = offsetRanges(TaskContext.get.partitionId)
            if (o.topic == "t1") {
              //hello_topic 处理逻辑
              println("t1 logic:" + s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
            }
            if (o.topic == "t2") {
              //hello_topic2 处理逻辑
              println("t2 logic:" + s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
            }
          }
        )
      }

    })

    stream.map(record => (record.key, record.value)).print()




    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
