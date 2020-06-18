package com.yiban.test

import org.apache.spark.sql.SparkSession
import org.junit.Test

@Test
class ScalaDemo1 {

  @Test
  def test1() ={
    println((math.log(4)*20).toInt)
  }

  @Test
  def test2(): Unit = {
    val str = "2018-02-22T00:00:00+08:00|~|200|~|/test?pcid=DEIBAH&siteid=3"
    val arr: Array[String] = str.split("\\|~\\|", -1)
    println(arr(2).split("[=|&]", -1).length)
    println(arr(2).split("[=|&]", -1)(1))
    println(arr(2).split("[=|&]", -1)(3))
  }

  @Test
  def test3() = {
    val spark = SparkSession
      .builder()
      .appName("test")
      .master("local[*]")
      .getOrCreate()
  }

  @Test
  def test4() ={
    val sonClass = new SonClass
  }

}
