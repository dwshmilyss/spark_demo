package com.yiban.spark.streaming.dev.kafka10.example_redis

import java.util

import com.yiban.spark.streaming.dev.utils.ConfigUtils
import com.yiban.spark.streaming.dev.utils.ConfigUtils.Config
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Pipeline

object TestSparkStreaming {

  val logger:Logger = Logger.getLogger(TestSparkStreaming.getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val kafkaConfig:Config = ConfigUtils.loadProperties("kafka.properties")
    val brokers = kafkaConfig.getString("brokers")
    val topic = kafkaConfig.getString("topic")
    val groupId = kafkaConfig.getString("groupId")
    val partition:Int = 0//只有一个分区

    val enable_auto_commit = kafkaConfig.getString("enable.auto.commit")
    val auto_offset_reset= kafkaConfig.getString("auto.offset.reset")
    val key_deserializer= kafkaConfig.getString("key.deserializer")
    val value_deserializer= kafkaConfig.getString("value.deserializer")

    //kafka参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> key_deserializer,
      "value.deserializer" -> value_deserializer,
      "group.id" -> groupId,
      "enable.auto.commit" -> enable_auto_commit,
      "auto.offset.reset" -> auto_offset_reset
    )

    val redisConfig:Config = ConfigUtils.loadProperties("redis.properties")
    //redis configuration
    val maxTotal = redisConfig.getInt("maxTotal")
    val maxIdle = redisConfig.getInt("maxIdle")
    val minIdle = redisConfig.getInt("minIdle")
    val redisHost = redisConfig.getString("redisHost")
    val redisPort = redisConfig.getInt("redisPort")
    val redisTimeout = redisConfig.getInt("redisTimeout")
    //默认DB 用户存放offset和pv数据
    val dbDefaultIndex = redisConfig.getInt("dbDefaultIndex")
    InternalRedisClient.makePool(redisHost,redisPort,redisTimeout,maxTotal,maxIdle,minIdle)

    val conf = new SparkConf().setAppName("TestSparkStreamingByRedis").setIfMissing("spark.master","local[*]")
    val ssc = new StreamingContext(conf,Seconds(10))

    //从redis获取上一次消费的offset
    val jedis = InternalRedisClient.getPool.getResource
    jedis.select(dbDefaultIndex)
    val topic_partition_key = topic + "_" + partition
    var lastOffset = 2102l//设置初始的offset 注意该值一定不能比当前topic_patition的最小值还小，否则会报错
    val lastSaveOffset = jedis.get(topic_partition_key)
    jedis.close()//注意这里不是关闭连接，在JedisPool模式下，Jedis会被归还给资源池。


    var recordDStream: InputDStream[ConsumerRecord[String, String]] = if (null != lastSaveOffset) {//如果redis中获取到了当前的offset，就从offset开始处理
      try {
        lastOffset = lastSaveOffset.toLong
      } catch {
        case exception: Exception => println(exception.getMessage)
          println("get lastSaveOffset error,lastSaveOffset from redis [" + lastSaveOffset + "]")
          System.exit(1)
      }
      println("lastOffset from redis -> " + lastOffset)

      //设置每个分区的起始offset
      val fromOffsets = Map{new TopicPartition(topic,partition) -> lastOffset}

      KafkaUtils.createDirectStream[String,String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParams,fromOffsets)
      )
    } else{//如果没有获取到offset 就从auto.offset.reset的配置开始处理
      KafkaUtils.createDirectStream[String,String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParams)
      )
    }


    //使用DirectApi创建stream
//    val stream = KafkaUtils.createDirectStream[String,String](
//      ssc,
//      LocationStrategies.PreferConsistent,
//      ConsumerStrategies.Assign[String,String](fromOffsets.keys.toList,kafkaParams,fromOffsets)
//    )



    //开始处理批次消息
    recordDStream.foreachRDD{
      rdd =>
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        val result = processLogs(rdd)
        println("============= Total " + result.length + "events in this batch ....")
        //这里必须重新声明变量，因为上面的jedis是在driver中，而这里的代码是在executor中执行的
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


        //更新offset 这里是更新到redis
        offsetRanges.foreach{
          offsetRange =>
            println("partition:"+offsetRange.partition + ",fromOffset:" + offsetRange.fromOffset + ",untilOffset:" + offsetRange.untilOffset)
            val topic_partition_key  = offsetRange.topic + "_" + offsetRange.partition
            p1.set(topic_partition_key,offsetRange.untilOffset + "")
        }
        //也可以手动提交
//        recordDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)


        //提交事务
        p1.exec()
        p1.sync()//关闭pipeline

        jedis.close()
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
