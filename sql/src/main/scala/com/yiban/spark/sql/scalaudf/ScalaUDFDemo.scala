package com.yiban.spark.sql.scalaudf

import org.apache.spark.sql.SparkSession


/**
  * 用户定义函数（User-defined functions, UDFs）
  */
object ScalaUDFDemo {

  val spark = SparkSession
    .builder()
    .appName("scala UDFs")
    .master("local[*]")
    .getOrCreate()


  val sqlContext = spark.sqlContext


  def main(args: Array[String]) {
    val path = """file:///D:/source_code/sparkDemo/sql/src/main/resources/data/temperatures.json"""
    val df = spark.read.json(path)
    df.createOrReplaceTempView("citytemps")
    spark.udf.register("CTOF",(degreesCeleius:Double) => ((degreesCeleius * 9.0 / 5.0) + 32.0))

//    spark.sql("SELECT city, CTOF(avgLow) AS avgLowF, CTOF(avgHigh) AS avgHighF FROM citytemps").show()
    spark.sql("SELECT UPPER(city), avgLow, avgHigh FROM citytemps").show()
  }
}
