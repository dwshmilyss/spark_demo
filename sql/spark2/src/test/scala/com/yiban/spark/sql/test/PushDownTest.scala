package com.yiban.spark.sql.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.{BeforeEach, Test}


class PushDownTest {
  @BeforeEach
  def init():Unit = {
    val logger: Logger = Logger.getLogger("org.apache.spark")
    Logger.getLogger("org").setLevel(Level.ERROR)
  }

  /**
   *
   * == Analyzed Logical Plan ==
    name: string
    Project [name#2]
    +- Union
       :- LocalRelation [name#2, age#3L]
       :- LocalRelation [name#7, age#8L]
       +- LocalRelation [name#2, age#3L]

    == Optimized Logical Plan ==
    Union
    :- LocalRelation [name#2]
    :- LocalRelation [name#7]
    +- LocalRelation [name#2]
   可以看到 Optimized 后就只剩 name 一列了
   */
  @Test
  def testPushDownProjectionUnion():Unit = {
    val sparkSession = SparkSession.builder()
      .appName("testPushDownProjection")
      .master("local[2]")
      .getOrCreate()
    import sparkSession.implicits._
    val ds2 = Seq(Person("Michael", 29), Person("Andy", 30), Person("Justin", 11)).toDS()
    val ds3 = Seq(Person("Michael", 9), Person("Andy", 7), Person("Justin", 11)).toDS()

    // 进行数据集的union和select运算
    ds2.union(ds3).union(ds2).select("name").explain(true)
  }

  /**
   * == Analyzed Logical Plan ==
    _id: bigint
    Filter (_id#2L = cast(0 as bigint))
    +- Project [id#0L AS _id#2L]
       +- Range (0, 2, step=1, splits=Some(2))

    == Optimized Logical Plan ==
    Project [id#0L AS _id#2L]
    +- Filter (id#0L = 0)
       +- Range (0, 2, step=1, splits=Some(2))

   可以看到Optimized后filter移到了project的前面执行
   */
  @Test
  def testPushDownProjection():Unit = {
    val spark = SparkSession.builder()
      .appName("testPushDownProjection")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val ds = spark.range(2)
    ds.printSchema()
    ds.select('id as "_id").filter('_id === 0).explain(true)
  }
}
