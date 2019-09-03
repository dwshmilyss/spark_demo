package com.yiban.spark.sql.dev

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField}

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
    test1()
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

  //这里的Person不能放在test1()里面
  case class Person(name: String, age: Long)
  def test1() = {
    val caseClassDS = Seq(Person("Andy",32)).toDS()
    caseClassDS.show()
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect()
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.printSchema()
    peopleDS.map(person => Person(person.name,person.age)).printSchema()

    peopleDS.createOrReplaceTempView("people")
    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")
    teenagersDF.map(teenager => "age : " + teenager(1)).show()
    teenagersDF.map(teenager => "name: " + teenager.getAs[String]("name")).show()

    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
  }

  def test2() = {
    import org.apache.spark.sql.types._
    val peopleRDD = spark.sparkContext.textFile(path)
    val schemaString = "name age"
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName,StringType,nullable = true))
    val schema = StructType(fields)
    val rowRDD = peopleRDD.map(_.split(",")).map(row => Row(row(0),row(1).trim))
    val peopleDF = spark.createDataFrame(rowRDD,schema)
  }
}
