package com.yiban.spark.streaming.dev.kafka8.demo1

import com.yiban.spark.streaming.dev.kafka8.demo1.KafkaCluster.LeaderOffset
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkException}

object KafkaByZk1 {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.exit(1);
    }
    //支持一个topic 也可以有多个 topic.split(",").toSet，这里先测试一个
    val Array(brokers, topic, groupId) = args
    //    val topics = Set[String]("test")
//    val groupId = ""
    val kafkaParams =  Map[String, String](
        "metadata.broker.list" -> brokers,
        "group.id " -> groupId,
        "fetch.message.max.bytes" -> "20971520",
        "auto.offset.reset" -> "smallest"
      )

    val kafkaCluster = new KafkaCluster(kafkaParams)
    val messages = createDirectStream(kafkaCluster,Set(topic),groupId)
    messages.foreachRDD{
          //添加业务逻辑
      rdd => ""
      // 更新offsets
      updateZKOffsets(rdd,kafkaCluster)
    }

  }




  def createDirectStream(kafkaCluster: KafkaCluster, topics: Set[String], groupId: String):InputDStream[(String,String)] = {
    val sc = new SparkContext()
    val ssc = new StreamingContext(sc, Seconds(10))
    val messages = {
      setOrUpdateOffsets(kafkaCluster,topics,groupId)
      val kafkaPartitionsE: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(topics)
      if (kafkaPartitionsE.isLeft) {
        throw new SparkException("get kafka partition failed")
      }
      val topicAndPartitions: Set[TopicAndPartition] = kafkaPartitionsE.right.get
      val consumerOffsetsE: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluster.getConsumerOffsets(groupId, topicAndPartitions)
      if (consumerOffsetsE.isLeft) {
        throw new SparkException("get kafka consumer offsets failed")
      }
      val consumerOffsets: Map[TopicAndPartition, Long] = consumerOffsetsE.right.get
      consumerOffsets.foreach {
        case (topicAndPartition: TopicAndPartition, offset: Long) => println("topic = " + topicAndPartition.topic + ",partition = " + topicAndPartition.partition + ",offset = " + offset)
      }
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
        ssc, kafkaCluster.kafkaParams, consumerOffsets, (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())
      )
    }
    messages
  }


  def setOrUpdateOffsets(kafkaCluster: KafkaCluster,topics:Set[String],groupId:String)={
    topics.foreach{
      topic => {
        println("current topic = " + topic)
        var hasConsumed = true
        val kafkaPartitionsE = kafkaCluster.getPartitions(Set(topic))
        if (kafkaPartitionsE.isLeft) {
          throw new SparkException("get kafka partition failed")
        }
        val kafkaPartitions = kafkaPartitionsE.right.get
        val consumerOffsetsE = kafkaCluster.getConsumerOffsets(groupId, kafkaPartitions)
        if (consumerOffsetsE.isLeft) hasConsumed = false
        if (hasConsumed) {
          /**
            * 如果有消费过，有两种可能，如果streaming程序执行的时候出现kafka.common.OffsetOutOfRangeException，
            * 说明zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
            * 针对这种情况，只要判断一下zk上的consumerOffsets和earliestLeaderOffsets的大小，如果consumerOffsets比earliestLeaderOffsets还小的话，
            * 说明是过时的offsets,这时把earliestLeaderOffsets更新为consumerOffsets
            */
          val earliestLeaderOffsets:Map[TopicAndPartition, LeaderOffset] = kafkaCluster.getEarliestLeaderOffsets(kafkaPartitions).right.get
          println(" earliestLeaderOffsets = " + earliestLeaderOffsets)
          var consumerOffsets:Map[TopicAndPartition,Long] = consumerOffsetsE.right.get
          //判断是否有过时的offset
          val flag = consumerOffsets.forall{
            //只有所有的partition的offset >= leaderEarliestOffsets 的时候才返回true, 即没有过时的offset
            case (topicAndPartition: TopicAndPartition,consumerOffset: Long) => consumerOffset >= earliestLeaderOffsets(topicAndPartition).offset
          }
          if (!flag) {
            //如果有过时的offset
            println("consumer group = " + groupId + " offset已经过时，更新为leaderEarliestOffsets")
            consumerOffsets.foreach{
              case (topicAndPartition: TopicAndPartition,consumerOffset:Long) =>
                val earliestLeaderOffset:Long = earliestLeaderOffsets(topicAndPartition).offset
                if (consumerOffset < earliestLeaderOffset) {//哪个过时更新哪个
                  println("consumer group:" + groupId + ",topic:" + topicAndPartition.topic + ",partition:" + topicAndPartition.partition +
                    " offsets已经过时，更新为" + earliestLeaderOffset)
                  consumerOffsets += (topicAndPartition -> earliestLeaderOffset)
                }
            }
          }
          if (!consumerOffsets.isEmpty) {
            kafkaCluster.setConsumerOffsets(groupId,consumerOffsets)
          }
        }else{//如果没消费过
          //获取配置文件中的offset设置
          val reset = kafkaCluster.kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
            var leaderOffsets: Map[TopicAndPartition, LeaderOffset] = null
            if (reset == Some("smallest")) {//如果是从最小的offset开始
              val leaderOffsetsE = kafkaCluster.getEarliestLeaderOffsets(kafkaPartitions)
              if (leaderOffsetsE.isLeft)
                throw new SparkException(s"get earliest leader offsets failed: ${leaderOffsetsE.left.get}")
              leaderOffsets = leaderOffsetsE.right.get
            } else {//否则就从最后一次的offset开始
              val leaderOffsetsE = kafkaCluster.getLatestLeaderOffsets(kafkaPartitions)
              if (leaderOffsetsE.isLeft)
                throw new SparkException(s"get latest leader offsets failed: ${leaderOffsetsE.left.get}")
              leaderOffsets = leaderOffsetsE.right.get
            }
            val offsets = leaderOffsets.map {
              case (tp, offset) => (tp, offset.offset)
            }
            kafkaCluster.setConsumerOffsets(groupId, offsets)
        }
      }
    }
  }


  /**
    * 更新zookeeper上的消费offsets
    * @param rdd
    */
  def updateZKOffsets(rdd: RDD[(String, String)],kafkaCluster: KafkaCluster) : Unit = {
    val groupId = kafkaCluster.kafkaParams.get("group.id").get
    val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

    for (offsets <- offsetsList) {
      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
      val o = kafkaCluster.setConsumerOffsets(groupId, Map((topicAndPartition, offsets.untilOffset)))
      if (o.isLeft) {
        println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
      }
    }
  }

}
