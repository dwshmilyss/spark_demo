package com.yiban.spark.core.dev

import org.apache.spark.{SparkContext, SparkConf}

object ShuffleDemo {

  val conf = new SparkConf().setMaster("local[*,4]").setAppName("accumulator")
  val sc = new SparkContext(conf)

  /**
    * 处理数据倾斜
    */
  def testShuffle1() = {

  }
}
