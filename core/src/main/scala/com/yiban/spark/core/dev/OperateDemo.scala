package com.yiban.spark.core.dev

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, explode_outer}

object OperateDemo {

  case class Person(id:Int,name:String,address:Array[String])
  val spark: SparkSession = SparkSession.builder()
    .appName("OkhttpDemo")
    .master("local[2]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
//    testExplode()
    testExplodeOuter()
  }

  def testExplode()={
    val df = spark.createDataFrame(List(Person(1,"dww",Array(null,null)),Person(1,"zs",Array("anhui","hefei"))))
    val df1 = df.withColumn("explode_address",explode(col("address")))
    df.schema.printTreeString()
    df1.schema.printTreeString()
    df.show()
    df1.show()
  }

  def testExplodeOuter()={
    val df = spark.createDataFrame(List(Person(1,"dww",Array(null,null)),Person(1,"zs",Array("anhui","hefei"))))
    val df1 = df.withColumn("explode_address",explode_outer(col("address")))
    df.schema.printTreeString()
    df1.schema.printTreeString()
    df.show()
    df1.show()
  }
}
