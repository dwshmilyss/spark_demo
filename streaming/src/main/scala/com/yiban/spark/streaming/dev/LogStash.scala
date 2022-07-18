package com.yiban.spark.streaming.dev

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.parsing.json.JSON

/**
 * Created by 10000347 on 2015/7/10.
 */
object LogStash {

  case class LogStashV1(message:String, path:String, host:String, lineno:Double, timestamp:String)
  case class Status(sum:Double = 0.0, count:Int = 0) {
    val avg = sum / scala.math.max(count, 1)
    var countTrend = 0.0
    var avgTrend = 0.0
    def +(sum:Double, count:Int): Status = {
      val newStatus = Status(sum, count)
      if (this.count > 0 ) {
        newStatus.countTrend = (count - this.count).toDouble / this.count
      }
      if (this.avg > 0 ) {
        newStatus.avgTrend = (newStatus.avg - this.avg) / this.avg
      }
      newStatus
    }
    override def toString = {
      s"Trend($count, $sum, $avg, $countTrend, $avgTrend)"
    }
  }

  def updatestatefunc(newValue: Seq[(Double, Int)], oldValue: Option[Status]): Option[Status] = {
    val prev = oldValue.getOrElse(Status())
    var current = prev + ( newValue.map(_._1).sum, newValue.map(_._2).sum )
    Some(current)
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("LogStash")
    val sc  = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(args(0).toInt))
    val lines = ssc.socketTextStream(args(1), args(2).toInt)
    //是把json格式转换成map<k,v>
    val jsonf = lines.map(JSON.parseFull(_)).map(_.get.asInstanceOf[scala.collection.immutable.Map[String, Any]])

    //把map<k,v>转换成LogStashV1对象
    val logs = jsonf.map(data => LogStashV1(data("message").toString, data("path").toString, data("host").toString, data("lineno").toString.toDouble, data("@timestamp").toString))

    //过滤条件
    val r = logs.filter(a => a.path.equals("/var/log/system.log")).filter(a => a.lineno > 70)

    r.map(a => a.message -> (a.lineno, 1)).reduceByKey((a, b) => {
      (a._1 + b._1, a._2 + b._2)
    }).updateStateByKey(updatestatefunc).filter(t => t._2.avgTrend.abs > 0.1).print()

//    jsonf.filter(l => l("lineno") == 75).window(Seconds(args(3).toInt)).foreachRDD( rdd => {
//      rdd.foreach( r => {
//        println(r("path"))
//      })
//    })

    ssc.start()
    ssc.awaitTermination()
  }
}
