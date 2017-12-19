package com.yiban.spark.core.dev

import org.apache.spark.{SparkContext, SparkConf}

object RemoteDebug {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","G:\\soft_by_work\\hadoop_soft\\hadoop-2.6.0")
//    val conf = new SparkConf().setAppName("Spark Pi").setMaster("spark://master01:7077").setJars(List("/root/core.jar"))
//    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local")
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("spark://10.21.3.73:7077")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = 100000 * slices
    val count = spark.parallelize(1 to n, slices).map { i =>
      val x = scala.math.random * 2 - 1
      val y = scala.math.random * 2 - 1
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
}
