package com.yiban.spark.streaming.dev

import java.sql.{Connection, DriverManager}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by 10000347 on 2015/7/6.
 */
object StreamingBySocketAndStateful {

  def createContext(checkpointDirectory: String): StreamingContext  = {
    val sparkConf = new SparkConf().setAppName("StreamingBySocketAndStateful").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(10));
    //使用updateStateByKey前需要设置checkpoint
    //    ssc.checkpoint("hdfs://jobtracker:9000/spark/checkpoint")
    ssc.checkpoint(checkpointDirectory)
    val addFunc = (currValues: Seq[Int], prevValueState: Option[Int]) => {
      //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
      val currentCount = currValues.sum
      // 已累加的值
      val previousCount = prevValueState.getOrElse(0)
      // 返回累加后的结果，是一个Option[Int]类型
      Some(currentCount + previousCount)
    }
    // 获得一个DStream负责连接 监听端口:地址
    val lines = ssc.socketTextStream("10.21.3.126", 9999);
    // 对每一行数据执行Split操作
    val words = lines.flatMap(_.split(" "));
    // 统计word的数量
    val pairs = words.map(word => (word, 1));
    val totalWordCounts = pairs.updateStateByKey[Int](addFunc)

    //    结果存入MYSQL
    //    totalWordCounts.foreachRDD(rdd => {
    //      //embedded function
    //      def func(records: Iterator[(String,Int)]) {
    //        println("records = "+records)
    //        var conn: Connection = null
    //        var stmt: PreparedStatement = null
    //        try {
    //          val url = "jdbc:mysql://10.21.3.120:3306/yiban_BI?characterEncoding=utf-8";
    //          val user = "root";
    //          val password = "wenha0"
    //          conn = DriverManager.getConnection(url, user, password)
    //          records.foreach(word => {
    //            println("word = "+word)
    //            val select_sql = "select name from test_person where name = ?"
    //            stmt = conn.prepareStatement(select_sql)
    //            stmt.setString(1, word._1)
    //            val rs = stmt.executeQuery()
    //            if (rs.next()){
    //              val insert_sql = "update test_person set age = ? where name = ?"
    //              stmt = conn.prepareStatement(insert_sql)
    //              stmt.setInt(1, word._2)
    //              stmt.setString(2, word._1)
    //              stmt.executeUpdate()
    //            }else{
    //              val insert_sql = "insert into test_person(name,age) values (?,?)"
    //              stmt = conn.prepareStatement(insert_sql)
    //              stmt.setString(1, word._1)
    //              stmt.setInt(2, word._2)
    //              stmt.executeUpdate()
    //            }
    //          })
    //        } catch {
    //          case e: Exception => e.printStackTrace()
    //        } finally {
    //          if (stmt != null) {
    //            stmt.close()
    //          }
    //          if (conn != null) {
    //            conn.close()
    //          }
    //        }
    //      }
    //
    //      //      val repartitionedRDD = rdd.repartition(3)
    //      //      repartitionedRDD.foreachPartition(func)
    //      rdd.foreachPartition(func)
    //    })

    //另一种写法（和上面的写法效果一样）
    //    totalWordCounts.foreachRDD(rdd => {
    //      rdd.foreachPartition(records => {
    //        println("records = "+records)
    //        var conn: Connection = null
    //        var stmt: PreparedStatement = null
    //        try {
    //          conn = ConnectionPool.getConnection()
    //          records.foreach(word => {
    //            println("word = "+word)
    //            val select_sql = "select name from test_person where name = ?"
    //            stmt = conn.prepareStatement(select_sql)
    //            stmt.setString(1, word._1)
    //            val rs = stmt.executeQuery()
    //            if (rs.next()){
    //              val insert_sql = "update test_person set age = ? where name = ?"
    //              stmt = conn.prepareStatement(insert_sql)
    //              stmt.setInt(1, word._2)
    //              stmt.setString(2, word._1)
    //              stmt.executeUpdate()
    //            }else{
    //              val insert_sql = "insert into test_person(name,age) values (?,?)"
    //              stmt = conn.prepareStatement(insert_sql)
    //              stmt.setString(1, word._1)
    //              stmt.setInt(2, word._2)
    //              stmt.executeUpdate()
    //            }
    //          })
    //        } catch {
    //          case e: Exception => e.printStackTrace()
    //        } finally {
    //          if (stmt != null) {
    //            stmt.close()
    //          }
    //          ConnectionPool.closeConnection()
    //        }
    //      })
    //    })


    // 输出结果
    totalWordCounts.print();
    ssc
  }

  def main(args: Array[String]) {
//    if (args.length < 2) {
//      System.err.println("Usage: NetworkWordCount <hostname> <port>")
//      System.exit(1)
//    }

    val checkpointDirectory = "./"
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => {createContext(checkpointDirectory)}
    )



    // 输出结果
    ssc.start();             // 开始
    ssc.awaitTermination();  // 计算完毕退出
  }
}

object ConnectionPool{
  val url = "jdbc:mysql://10.21.3.120:3306/yiban_BI?characterEncoding=utf-8";
  val user = "root";
  val password = "wenha0"
  var conn: Connection = _


  def getConnection(): Connection = {
    conn = DriverManager.getConnection(url, user, password)
    conn
  }

  def closeConnection(): Unit ={
    if (conn != null) {
      conn.close()
    }
  }
}
