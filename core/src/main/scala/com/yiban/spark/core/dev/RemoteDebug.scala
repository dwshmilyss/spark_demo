package com.yiban.spark.core.dev

import org.apache.spark.deploy.SparkSubmit
import org.apache.spark.{SparkContext, SparkConf}

object RemoteDebug {
  def main(args: Array[String]) {
    //    System.setProperty("hadoop.home.dir","G:\\soft_by_work\\hadoop_soft\\hadoop-2.6.0")
    //    val conf = new SparkConf().setAppName("Spark Pi").setMaster("spark://master01:7077").setJars(List("/root/core.jar"))
    //    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local")
    //    //这个可以直接远程调试，集群无需做特殊配置
    val conf = new SparkConf().setAppName("Spark Pi")
      .setMaster("spark://master01:7077")
      .setJars(List("D:\\source_code\\spark_demo\\out\\artifacts\\core_jar\\core.jar"))
      .setIfMissing("spark.driver.host", "10.21.128.49")
      .setIfMissing("spark.driver.port", "9089")
//    conf.set("spark.executor.memory", "10g")
//    conf.set("spark.executor.cores", "8")
//    conf.set("spark.total.executor.cores", "32")
    val sc = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = 100000 * slices
    val count = sc.parallelize(1 to n, slices).map { i =>
      val x = scala.math.random * 2 - 1
      val y = scala.math.random * 2 - 1
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    sc.stop()
  }
}
