package com.yiban.spark.sql.dev

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * 小表总体估计大小超过spark.sql.autoBroadcastJoinThreshold设定的值，即不满足broadcast join条件
  * 开启尝试使用hash join的开关，spark.sql.join.preferSortMergeJoin=false
  * 每个分区的平均大小不超过spark.sql.autoBroadcastJoinThreshold设定的值，即shuffle read阶段每个分区来自小表的记录要能放到内存中
  * 大表的大小是小表三倍以上
  *
  *
  */
object SparkSqlJoinDemo {

  val logger: Logger = Logger.getLogger("org.apache.spark")
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "G:\\soft\\hadoop-2.8.2\\hadoop-2.8.2")

    val spark = SparkSession.builder().appName("SparkSqlJoinDemo").master("local[*]").getOrCreate()

    import spark.implicits._

    //-1是禁用广播
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.join.preferSortMergeJoin", false)
    // 每个分区的平均大小不超过spark.sql.autoBroadcastJoinThreshold设定的值
    println(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

    val df1 : DataFrame = Seq(
      ("0","a"),
      ("0","a"),
      ("0","a"),
      ("0","a"),
      ("1","b"),
      ("1","b"),
      ("1","b"),
      ("1","b"),
      ("2","c"),
      ("2","c"),
      ("2","c"),
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
