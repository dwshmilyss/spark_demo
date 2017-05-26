package com.yiban.spark.streaming.dev.kafka8

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by 10000347 on 2017/4/7.
  */
object ReadBySureOffsetDemo {
  val logger: Logger = LoggerFactory.getLogger("org.apache.spark")

  def main(args: Array[String]) {
    //设置打印日志级别
    org.apache.log4j.Logger.getLogger("org.apache.kafka").setLevel(org.apache.log4j.Level.ERROR)
    org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(org.apache.log4j.Level.ERROR)
    //    org.apache.log4j.Logger.getLogger("org.apache.spark").setLevel(org.apache.log4j.Level.ERROR)
    println("main programming beginning ...")
    logger.warn("main programming beginning ...")
    if (args.length < 1) {
      logger.error("Your arguments were " + args.mkString(","))
      System.exit(1)
    }
    val Array(brokers, topics, group, offset, checkpointDirectory, batch) = args
    println("checkpoint = " + checkpointDirectory)
    logger.warn("checkpoint = " + checkpointDirectory)
    //创建 StreamingContext
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => {
        createContext(checkpointDirectory, brokers, topics, group, offset.toLong, batch.toInt)
      })

    ssc.start()
    ssc.awaitTermination()
  }

  def createContext(checkpointDirectory: String, brokers: String, topics: String, group: String, offset: Long, batch: Int): StreamingContext = {
    //    val brokers: String = "10.21.3.129:9092"
    //    val group: String = "g4"
    //    val offset: Long = 200l
    //    val batch: Int = 5

    // 创建上下文
    val sparkConf = new SparkConf()
    val ssc = new StreamingContext(sparkConf, Seconds(batch))
    if (checkpointDirectory != null && !"".equals(checkpointDirectory)) {
      ssc.checkpoint(checkpointDirectory)
    }

    //    val ssc = createContext(checkpointDirectory, batch.toInt)
    println("StreamingContext created.....")
    logger.warn("StreamingContext created.....")
    val directKafkaStream: InputDStream[(String, String)] = createDirectStream(ssc, brokers, topics, group, offset.toLong)
    println("InputDStream created.....")
    logger.warn("InputDStream created.....")

    val lines = directKafkaStream.map(line => {
      val key = line._1.toString
      val value = line._2.toString
      println(s"key = $key,value = $value")
      logger.warn(s"key = $key,value = $value")
      value
    })

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    //    wordCounts.print()

    //        //数据操作1
    //        wordCounts.foreachRDD(rdd => {
    //          val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //          rdd.foreachPartition(partitionOfRecords => {
    //    //        partitionOfRecords.foreach(pair => println(pair._1 + " : " + pair._2))
    //            while (partitionOfRecords.hasNext){
    //              val (k,v) = partitionOfRecords.next()
    //              println(k + " : " + v)
    //              logger.warn(k + " : " + v)
    //            }
    //            partitionOfRecords.foreach{
    //              x => {
    //                val o: OffsetRange = offsetsList(TaskContext.get.partitionId)
    //                println(x._1 + " : " + x._2)
    //                println(s"topic = ${o.topic} , partition = ${o.partition} , fromOffset = ${o.fromOffset} , untilOffset = ${o.untilOffset}")
    //                logger.warn(x._1 + " : " + x._2)
    //                logger.warn(s"topic = ${o.topic} , partition = ${o.partition} , fromOffset = ${o.fromOffset} , untilOffset = ${o.untilOffset}")
    //
    //              }
    //            }
    //          })
    //        })

    //    数据操作2
    directKafkaStream.foreachRDD(rdd => {
      println("1")
      //获取offset集合
      val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetsList.foreach(x => {
        println("offsetsList.length = " + offsetsList.length + ",topic = " + x.topic + ",partition = " + x.partition + ",fromOffset = " + x.fromOffset + ",untilOffset = " + x.untilOffset + ",rdd.partition = " + rdd.partitions.length)
      })
      rdd.foreachPartition(partitionOfRecords => {
        //        while (partitionOfRecords.hasNext) {
        //          val (k, v) = partitionOfRecords.next()
        //          println(k + " : " + v)
        //          logger.warn(k + " : " + v)
        //        }

        partitionOfRecords.foreach(line => {
          val o: OffsetRange = offsetsList(TaskContext.get.partitionId)
          println("++++++++++++++++++++++++++++++此处记录offset+++++++++++++++++++++++++++++++++++++++")
          println(s"topic = ${o.topic} , partition = ${o.partition} , fromOffset = ${o.fromOffset} , untilOffset = ${o.untilOffset}")
          println("+++++++++++++++++++++++++++++++此处消费数据操作++++++++++++++++++++++++++++++++++++++")
          println("The kafka  line is " + line._2)
        })
      })
    })

    ssc
  }

  def createDirectStream(ssc: StreamingContext, brokers: String, topics: String, group: String, offset: Long): InputDStream[(String, String)] = {
    // 创建包含brokers和topic的直接kafka流
    val topicsSet: Set[String] = topics.split(",").toSet
    //kafka配置参数
    val kafkaParams: Map[String, String] = Map[String, String](
      //      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      //      ConsumerConfig.GROUP_ID_CONFIG -> group,
      //      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getClass.getCanonicalName,
      //      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getClass.getCanonicalName
      "metadata.broker.list" -> brokers,
      "group.id" -> group
    )

    /**
      * topic，partition_no，offset
      * 从指定位置开始读取kakfa数据
      * 注意：由于Exactly  Once的机制，所以任何情况下，数据只会被消费一次！
      * 指定了开始的offset后，将会从上一次Streaming程序停止处，开始读取kafka数据
      */
    //    val offsetList = List((topics, 0, offset))
    /**
      * fromOffsets结构 Map(TopicAndPartition -> offset)
      */
    //    val fromOffsets = setFromOffsets(offsetList) //构建参数
    val messageHandler = (mam: MessageAndMetadata[String, String]) => (mam.topic, mam.message()) //构建MessageAndMetadata
    //使用高级API从指定的offset开始消费，欲了解详情，
    //请进入"http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.kafka.KafkaUtils$"查看
    //指定消费partition的offset
    //    val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    messages
  }


  //构建Map
  def setFromOffsets(list: List[(String, Int, Long)]): Map[TopicAndPartition, Long] = {
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    for (offset <- list) {
      val tp = TopicAndPartition(offset._1, offset._2) //topic和分区数
      fromOffsets += (tp -> offset._3) // offset位置
    }
    fromOffsets
  }
}
