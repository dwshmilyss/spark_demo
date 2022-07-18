package com.yiban.spark31.sql.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.junit.jupiter.api.{BeforeAll, BeforeEach, Test}

class TransfromTest {

  @BeforeEach
  def inti():Unit = {
    val logger: Logger = Logger.getLogger("org.apache.spark")
    Logger.getLogger("org").setLevel(Level.ERROR)
  }

  @Test
  def testFlatmap():Unit = {
    val spark = SparkSession.builder()
      .appName("testFlatmap")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val ds: Dataset[String] = Seq("hello spark","hello hadoop").toDS()
    ds.flatMap(item => item.split(" ")).show()
  }

  @Test
  def testMap():Unit = {
    val spark = SparkSession.builder()
      .appName("testMap")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val ds2 = Seq(Person("zhangsan",15),Person("lisi",20)).toDS()
    ds2.map(item => (Person)(item.name,item.age*2)).show()
  }

  @Test
  def testMappartition():Unit = {
    val spark = SparkSession.builder()
      .appName("testMappartition")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val ds2 = Seq(Person("zhangsan",15),Person("lisi",20)).toDS()
    ds2.mapPartitions(
      //iter 不能大到每个Executor的内存放不下，不然就会OOM
      //对每个元素进行转换，后生成一个新的集合
      iter => {
        val result = iter.map(item => Person(item.name,item.age*2))
        result
      }
    ).show()
  }

  @Test
  def testTransform():Unit = {
    val spark = SparkSession.builder()
      .appName("testMappartition")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val ds = spark.range(10)
    ds.show()
    ds.transform(item => item.withColumn("double",'id + 3)).show()
  }
}
