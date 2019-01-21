package com.yiban.spark.core.dev

import org.apache.spark.util.SizeEstimator
import org.apache.spark.{SparkConf, SparkContext}
import javax.servlet.http.HttpServletRequest
import org.apache.spark.storage.StorageLevel

object RDDMemoryDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("rdd_memory_demo")
    val sc = new SparkContext(conf)

    val data = sc.textFile("file:///d:/aaa.txt")
    val value = SizeEstimator.estimate(data)
    data.persist(StorageLevel.MEMORY_ONLY)
    data.count()
    println("size = " + value/1024)
    while(true){}
//    sc.stop()
  }
}
