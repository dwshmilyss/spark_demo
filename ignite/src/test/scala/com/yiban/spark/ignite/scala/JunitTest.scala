package com.yiban.spark.ignite.scala

import org.apache.spark.sql.SparkSession
import org.junit.{After, Before, Test}

class JunitTest {

  var spark: SparkSession = _

  @Before
  def init() = {
    println("before")
    spark = SparkSession.builder()
      .appName("ignite")
      .master("local")
      .config("spark.executor.instances", "2")
      .getOrCreate()
  }

  @Test
  def test() = {
    println("test")
    val path = new JunitTest().getClass.getClassLoader.getResource("local-config.xml").getPath
    println(path)
    val df = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test?user=root&password=120653")
      //.option("driver", "org.apache.ignite.IgniteJdbcDriver")
      .option("dbtable", "tb1").load()

    df.printSchema()
    df.show()
  }

  @After
  def close() = {
    println("after")
    spark.stop()
  }

}
