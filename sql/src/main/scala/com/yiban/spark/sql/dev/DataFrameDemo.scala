package com.yiban.spark.sql.dev

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.types.{DataType, DataTypes, IntegerType, StructField, StructType}

import scala.collection.JavaConverters.seqAsJavaListConverter


object DataFrameDemo {
  val logger: Logger = Logger.getLogger(DataFrameDemo.getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Person(name: String, sex: String, age: Int, height: Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DataFrameDemo")
      .setMaster("local[*]")

    val sparkSession = SparkSession.builder()
      .config(conf)
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()

    import sparkSession.implicits._
//    val person_json_path: String = DataFrameDemo.getClass.getClassLoader.getResource("data/person.json").getPath
//
//    val df = sparkSession.read.json(person_json_path)
//
//    df.printSchema()
//    df.show()
//    println(df.head(2).apply(0).getAs[String]("name"))
//
//    df.as[Person].head().age

    val row1:Row = new GenericRow(Array(1, "dw", 33, "male")).asInstanceOf[Row]
    val row2:Row = new GenericRow(Array(2, "zs", 30, "female")).asInstanceOf[Row]
    val row3:Row = new GenericRow(Array(3, "ll", 31, "female")).asInstanceOf[Row]
//    val rdd = sparkSession.sparkContext.makeRDD(List(row1,row2,row3))
//    val df = sparkSession.createDataFrame(rdd, StructType(Seq(StructField("id", DataTypes.IntegerType), StructField("name", DataTypes.StringType), StructField("age", DataTypes.IntegerType), StructField("gender", DataTypes.StringType))))

    val df = sparkSession.createDataFrame(List(row1,row2,row3).asJava,StructType(Seq(StructField("id",DataTypes.IntegerType),StructField("name",DataTypes.StringType),StructField("age",DataTypes.IntegerType),StructField("gender",DataTypes.StringType))))

    val res = df.collect()
    println(res(0))
//    res["col1"] = res["age"] + 1
//    df.show()

//    val rows1 = new GenericRowWithSchema(Array(1, 2, 3, 4).toArray[Any], StructType.apply(Seq(StructField("col1", IntegerType, true))))
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
