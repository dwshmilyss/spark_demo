package com.yiban.spark.sql.dev

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkSQLDemo {

  val logger: Logger = Logger.getLogger("org.apache.spark")
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("SparkSQLDemo")
    .master("local[*]")
    .config("", "")
    .getOrCreate()



  import spark.implicits._

  val path: String = SparkSQLDemo.getClass.getClassLoader.getResource("data/people.json").getPath

  def main(args: Array[String]): Unit = {
    test()
  }

  def test() = {

    val df = spark.read.json(path)
    df.show()
    df.printSchema()
    df.select("name").show()
    df.select($"name",$"age"+1).show()
    df.filter($"age" > 21).show()
    //这里只能筛选原始数据 加工后的数据用filter是无效的
    df.select($"name",$"age"+1).filter($"age" >= 19).show()
    df.groupBy("age").count().show()
    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("select * from people")
    sqlDF.show()
    println("====== GlobalTempView ======")
    df.createGlobalTempView("people")
    spark.sql("SELECT * FROM global_temp.people").show()
    spark.newSession().sql("SELECT * FROM global_temp.people").show()

  }
}
