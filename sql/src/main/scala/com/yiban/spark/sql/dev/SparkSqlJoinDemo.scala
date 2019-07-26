package com.yiban.spark.sql.dev

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object SparkSqlJoinDemo {

  val logger: org.slf4j.Logger = LoggerFactory.getLogger("org.apache.spark")
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "G:\\soft\\hadoop-2.8.2\\hadoop-2.8.2")

    val spark = SparkSession.builder().appName("SparkSqlJoinDemo").master("local[*]").getOrCreate()

    import spark.implicits._

    //-1是禁用广播
//    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    // 每个分区的平均大小不超过spark.sql.autoBroadcastJoinThreshold设定的值
//    spark.conf.set("spark.sql.join.preferSortMergeJoin", true)

    println(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

    val df1 : DataFrame = Seq(
      ("0","a"),
      ("1","b"),
      ("2","c")
    ).toDF("id","name")

    val df2 : DataFrame = Seq(
      ("0","d"),
      ("1","e"),
      ("2","f")
    ).toDF("aid","aname")

    //重新分区
//    df2.repartition()

    //join
    val res = df1.join(df2,$"id" === $"aid")

    res.explain()

    res.show()

    //释放资源
    spark.stop()
  }
}
