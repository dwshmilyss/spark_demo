package com.yiban.spark.sql.dev

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.types.{DataTypes, IntegerType, StructField, StructType}

object DataFrameDemo {
  val logger: Logger = Logger.getLogger(DataFrameDemo.getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Person(name:String,sex:String,age:Int,height:Int)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DataFrameDemo")
      .setMaster("local[*]")

    val sparkSession = SparkSession.builder()
      .config(conf)
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()

    import sparkSession.implicits._
    val person_json_path: String = DataFrameDemo.getClass.getClassLoader.getResource("data/person.json").getPath

    val df = sparkSession.read.json(person_json_path)

    df.printSchema()
    df.show()
    println(df.head(2).apply(0).getAs[String]("name"))

    df.as[Person].head().age

//    val rows = new GenericRow(Array(1,2,3,4).toArray[Any]).asInstanceOf[Row]
    val rows1 = new GenericRowWithSchema(Array(1,2,3,4).toArray[Any],StructType.apply(Seq(StructField("col1",IntegerType,true))))
//
//    println(rows.schema)
//    println(rows1.schema)
//    println(rows.getAs(0))
//    println(rows1.get(0))

//    df.show(2)
//
//    df.select("name", "age", "height").show(2)
//
//    import sparkSession.implicits._
//    //转换列类型
//    df.select($"name".cast(DataTypes.StringType)).printSchema()
//
//    //各种filter（如果非表达式的方式，那么等于用 "==="）
//    df.filter($"age" > 30 && $"sex" === "男").show()
//    df.filter("age > 30 and sex != '男'").show()
//    df.filter("age > 30 and sex == '男'").show()
//
//    //包含字符串
//    df.filter($"name".rlike("凡")).show()

    //group
//    df.groupBy("sex").count().show()
//    df.groupBy("sex").sum("age","height").show()
//    df.groupBy("sex").agg("age" -> "max","height" -> "sum").show()
//
//    val df1 = sparkSession.read.json(person_json_path)


//    df.join(df1,Seq("name","name"),"left").show()
//    df.join(df1,$"name" === $"name","left").show()

  }


}
