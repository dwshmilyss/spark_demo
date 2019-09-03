package com.yiban.spark.streaming.dev.kafka10.example_redis

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Pipeline

object TestSparkStreaming {
  def main(args: Array[String]): Unit = {
    val brokers = "10.21.3.73:9092"
    val topic = ""
    val partition:Int = 0//只有一个分区
    val start_offset:Long = 0l

    //kafka参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "auto.offset.reset" -> "none"
    )

    //redis configuration
    val maxTotal = 10
    val maxIdle = 10
    val minIdle = 1
    val redisHost = "10.21.3.77"
    val redisPort = 6379
    val redisTimeout = 30000
    //默认DB 用户存放offset和pv数据
    val dbDefaultIndex = 8
    InternalRedisClient.makePool(redisHost,redisPort,redisTimeout,maxTotal,maxIdle,minIdle)
    val conf = new SparkConf().setAppName("TestSparkStreamingByRedis").setIfMissing("spark.master","local[*]")
    val ssc = new StreamingContext(conf,Seconds(10))

    //从redis获取上一次消费的offset
    val jedis = InternalRedisClient.getPool.getResource
    jedis.select(dbDefaultIndex)
    val topic_partition_key = topic + "_" + partition
    var lastOffset = 0l
    val lastSaveOffset = jedis.get(topic_partition_key)

    if (null != lastSaveOffset) {
      try {
        lastOffset = lastSaveOffset.toLong
      } catch {
        case exception: Exception => println(exception.getMessage)
          println("get lastSaveOffset error,lastSaveOffset from redis [" + lastSaveOffset + "]")
          System.exit(1)
      }
    }
    InternalRedisClient.getPool.returnResource(jedis)
    println("lastOffset from redis -> " + lastOffset)

    //设置每个分区的起始offset
    val fromOffsets = Map{new TopicPartition(topic,partition) -> lastOffset}

    //使用DirectApi创建stream
    val stream = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Assign[String,String](fromOffsets.keys.toList,kafkaParams,fromOffsets)
    )

    //开始处理批次消息
    stream.foreachRDD{
      rdd =>
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        val result = processLogs(rdd)
        println("============= Total " + result.length + "events in this batch ....")
        val jedis = InternalRedisClient.getPool.getResource
        val p1 : Pipeline = jedis.pipelined()
        p1.select(dbDefaultIndex)
        p1.multi()//开启事务

        //逐条处理消息
        result.foreach{
          record =>
            //增加小时总pv
            val pv_by_hour_key = "pv_" + record.hour
            p1.incr(pv_by_hour_key)

            //增加网站小时pv
            val site_pv_by_hour_key = "site_pv_" + record.site_id + "_" + record.hour
            p1.incr(site_pv_by_hour_key)

            //使用set保存当天的uv
            val uv_by_day_key = "uv_" + record.hour.substring(0,10)
            p1.sadd(uv_by_day_key,record.user_id)
        }

        //更新offset
        offsetRanges.foreach{
          offsetRange =>
            println("partition:"+offsetRange.partition + ",fromOffset:" + offsetRange.fromOffset + ",untilOffset:" + offsetRange.untilOffset)
            val topic_partition_key  = offsetRange.topic + "_" + offsetRange.partition
            p1.set(topic_partition_key,offsetRange.untilOffset + "")
        }

        //提交事务
        p1.exec()
        p1.sync()//关闭pipeline

        InternalRedisClient.getPool.returnResource(jedis)
    }


    case class MyRecord(hour:String,user_id:String,site_id:String)

    def processLogs(messages:RDD[ConsumerRecord[String,String]]):Array[MyRecord] = {
      messages.map(_.value()).flatMap(parseLog).collect()
    }
    def parseLog(line:String):Option[MyRecord] = {
      val array : Array[String] = line.split("\\|~\\|",-1)

      try {
        val hour = array(0).substring(0, 13).replace("T", "-")
        val uri = array(2).split("[=|&]", -1)
        val user_id = uri(1)
        val site_id = uri(3)
        return Some(MyRecord(hour, user_id, site_id))
      } catch {
        case exception: Exception => println(exception.getMessage)
      }
      return None
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
