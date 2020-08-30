package com.yiban.spark.core.dev.dependency.narrow

import com.yiban.spark.core.dev.BaseSparkContext

/**
 * 子RDD的partition和父RDD的partition个数相同，只是父RDD是多个（union），例如：
 * 父RDD1的partition为2个
 * 父RDD2的partition为2个
 * 那么子RDD的partition个数为4个，索引从父RDD1开始为0,1；然后是父RDD2的索引2,3.
 */
object RangeDemo extends BaseSparkContext {
  def main(args: Array[String]): Unit = {
    val sparkContext = getLocalSparkContext("RangeDemo")
    //one to one
    val rdd1 = sparkContext.textFile("file:///d:/a.txt")
    val rdd2 = sparkContext.textFile("file:///d:/a.txt")

    val rdd3 = rdd1.union(rdd2)
    println("range dependency\n" + rdd3.collect().mkString("\n"))
    Thread.sleep(3600 * 1000)
    sparkContext.stop()
  }
}
