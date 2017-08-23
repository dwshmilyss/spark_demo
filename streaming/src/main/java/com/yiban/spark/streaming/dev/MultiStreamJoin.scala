package com.yiban.spark.streaming.dev

import org.apache.spark.SparkConf
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object MultiStreamJoin {

  def main(args: Array[String]) {
    //    val conf = new SparkConf().setMaster("local[*]").setAppName("join_test")
    val conf = new SparkConf().setMaster("spark://master01:7077").setAppName("join_test")
    val ssc = new StreamingContext(conf, Seconds(30))

    /**
      * 模拟创建黑名单 直接定义
      */
//    val blackList = Array(("hadoop", true), ("spark", true))
//    val blackListRDD = ssc.sparkContext.parallelize(blackList, 8)

    /**
      * 模拟创建黑名单 从HDFS中获取
      *
      */
//    val blackListDStream = ssc.textFileStream("/blackList").map {
//      x =>
//        val temp = x.split(" ")
//        (temp(1), temp(0).toInt)
//    }

    //使用自定义的textFileStream
    val blackListDStream = ssc.myTextFileStream("/blackList",Minutes(60)).map{
      x =>
        val temp = x.split(" ")
        (temp(1), temp(0).toInt)
    }
    blackListDStream.print()

    val adsClickStream = ssc.socketTextStream("10.21.3.73", 9999)

    /**
      * 模拟广告点击
      * 格式：time,name
      * return: name,(time,name)
      */
    val adsClickStreamMap = adsClickStream.map { ads => (ads.split(" ")(0), ads.split(" ")(1).toInt) }

//        val res = adsClickStreamMap.transform(userClickRDD => {
//          //通过leftOuterJoin操作 过滤用户点击数据中是黑名单的用户
//          val joinedBlackListRDD = userClickRDD.leftOuterJoin(blackListRDD)
//
//          val validClicked = joinedBlackListRDD.filter(joinedItem => {
//            if (joinedItem._2._2.getOrElse(false))
//              false
//            else
//              true
//          })
//          val res = validClicked.map(x => x._2._1)
//          res
//        })

    val res = adsClickStreamMap.transformWith(blackListDStream, (userClickRDD: RDD[(String,Int)], blackListRDD: RDD[(String,Int)]) => {
      //通过leftOuterJoin操作 过滤用户点击数据中是黑名单的用户
      val joinedBlackListRDD = userClickRDD.leftOuterJoin(blackListRDD)
      val validClicked = joinedBlackListRDD.filter(joinedItem => {
        if (joinedItem._2._2.getOrElse(0) == 0)
          true
        else
          false
      })
      val res = validClicked.map(x => (x._1,x._2._1))
      res
    })

    //打印 action触发job执行
    res.print()

    /**
      * 如果一些batch中没有数据 那么Spark Streaming将会产生EmptyRDD的RDD
      * 如果你想将接收到的Streaming数据写入HDFS中，当你调用foreachRDD的时候如果当前rdd是EmptyRDD，这样会导致在HDFS上生成大量的空文件
      * 这时候可以判断rdd的partitions是否为空，EmptyRDD的partitions肯定是空的
      * 也可以调用rdd.isEmpty()判断(spark-1.3.0后支持此函数)
      */
//    res.foreachRDD(rdd => {
//      if(!rdd.partitions.isEmpty){
//        rdd.saveAsTextFile(outputDir)
//      }
//    })
    ssc.start()
    ssc.awaitTermination()
  }
}
