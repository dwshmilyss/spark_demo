package com.yiban.spark.core.dev

import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}

object WordCountDemo extends Logging{
  def main(args: Array[String]) {
    //    System.setProperty("hadoop.home.dir","G:\\soft_by_work\\hadoop_soft\\hadoop-2.6.0")
    //    val conf = new SparkConf().setAppName("Spark Pi").setMaster("spark://master01:7077").setJars(List("/root/core.jar"))
    //    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local")
    //  如果要远程调试，则只需启动一个worker(10.21.3.75)
    val conf = new SparkConf().setAppName("WordCount")
      .setMaster("spark://master01:7077")
      .setJars(List("/root/jars/core.jar"))
    conf.set("spark.executor.memory", "10g")
    conf.set("spark.executor.cores", "8")
    conf.set("spark.total.executor.cores", "8")
    val sc = new SparkContext(conf)
    //设置日志级别，如果和log4j.properties里面的冲突的话，以log4j.properties为准
    sc.setLogLevel("WARN")
    val rdd1 = sc.textFile("hdfs://master01:9000/data/wordcount.txt")
    val rdd2 = rdd1.flatMap(_.split(" "))
    val rdd3 = rdd2.map((_,1))
    val rdd4 = rdd3.reduceByKey(_+_)
    rdd4.collect().foreach(println)
    rdd4.collect().foreach{
      line => logInfo(s"value = ${line}")
    }
    sc.stop()
  }
}
