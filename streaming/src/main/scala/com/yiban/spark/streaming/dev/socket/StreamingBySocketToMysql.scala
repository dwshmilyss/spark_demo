package com.yiban.spark.streaming.dev.socket

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by 10000347 on 2015/7/6.
 */
object StreamingBySocketToMysql {

  val logger = Logger.getLogger("org.apache.spark")
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("StreamingBySocketToMysql").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(10));
    // 获得一个DStream负责连接 监听端口:地址
    val dstream = ssc.socketTextStream("localhost", 9999);


    ssc.checkpoint("./checkpoint")
    val addFunc = (currValues: Seq[Int], prevValueState: Option[Int]) => {
      //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
      val currentCount = currValues.sum
      // 已累加的值
      val previousCount = prevValueState.getOrElse(0)
      // 返回累加后的结果，是一个Option[Int]类型
      Some(currentCount + previousCount)
    }

    dstream.foreachRDD(rdd => {
      //embedded function
      def func(records: Iterator[String]) {
        println("records = "+records)
        var conn: Connection = null
        var stmt: PreparedStatement = null
        try {
          val url = "jdbc:mysql://10.21.3.120:3306/yiban_BI?characterEncoding=utf-8";
          val user = "root";
          val password = "wenha0"
          conn = DriverManager.getConnection(url, user, password)
          records.flatMap(_.split(" ")).foreach(word => {
            println("word = "+word)
            val sql = "insert into test_pro(name) values (?)";
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, word)
            stmt.executeUpdate();
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          if (stmt != null) {
            stmt.close()
          }
          if (conn != null) {
            conn.close()
          }
        }
      }

//      val repartitionedRDD = rdd.repartition(3)
//      repartitionedRDD.foreachPartition(func)
      rdd.foreachPartition(func)
    })

    ssc.start();             // 开始
    ssc.awaitTermination();  // 计算完毕退出
  }
}
