package com.yiban.spark.core.dev

import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}

object HDFSDemo extends Logging{
  def main(args: Array[String]) {
        System.setProperty("hadoop.home.dir","D:\\Program Files\\hadoop-2.8.5")
    //    val conf = new SparkConf().setAppName("Spark Pi").setMaster("spark://master01:7077").setJars(List("/root/core.jar"))
    //    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local")
    //  如果要远程调试，则只需启动一个worker(10.21.3.75)
    val conf = new SparkConf().setAppName("HDFSDemo")
      .setMaster("spark://master01:7077")
//      .setJars(List("D:\\source_code\\spark_demo\\out\\artifacts\\core_jar\\core.jar"))
      .setJars(List("/root/jars/core.jar"))
    conf.set("spark.executor.extraJavaOptions","-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=10003")
    conf.set("spark.driver.extraJavaOptions","-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=10002")
//    conf.set("spark.driver.host", "10.21.128.49")
//    conf.set("spark.driver.bindAddress", "10.21.128.49")
//    conf.set("spark.driver.port", "4040")
    val sc = new SparkContext(conf)
    //设置日志级别，如果和log4j.properties里面的冲突的话，以log4j.properties为准
    sc.setLogLevel("WARN")
//    val rdd1 = sc.textFile("file:///d:/a.txt")
    val rdd1 = sc.textFile("hdfs://master01:9000/data/a.txt")
    val res = rdd1.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    res.collect().foreach(println)
//    rdd1.collect().foreach{
//      line => logInfo(s"value = ${line}")
//    }
    sc.stop()
  }
}
