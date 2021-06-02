package com.yiban.spark.core.dev.dependency.shuffle

import com.yiban.spark.core.dev.BaseSparkContext

object ReduceByKeyDemo extends BaseSparkContext {
  def main(args: Array[String]): Unit = {
    val sparkContext = getLocalSparkContext("ReduceByKeyDemo")

//    val rdd1 = sparkContext.parallelize(Seq(("c", 1), ("b", 1), ("a", 1), ("a", 1)), 2)
    val rdd1 = sparkContext.parallelize(Seq(("a", 1), ("b", 1), ("c", 1), ("a", 1)), 2)
    val rdd3 = rdd1.repartition(2)
    val rdd2 = rdd3.reduceByKey(_ + _)

    println("reduceByKey \n" + rdd2.collect().mkString("\n"))
    Thread.sleep(3600 * 1000)
    sparkContext.stop()
  }
}
