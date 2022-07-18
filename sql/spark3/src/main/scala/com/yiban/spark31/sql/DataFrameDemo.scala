package com.yiban.spark31.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.{array_distinct, collect_list, collect_set}
import org.apache.spark.sql.types.{DataTypes, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Encoders, Row, SparkSession}

import scala.collection.JavaConverters.seqAsJavaListConverter

object DataFrameDemo {
  val logger: Logger = Logger.getLogger(DataFrameDemo.getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Person(name: String, sex: String, age: Int, height: Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DataFrameDemo")
      .setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()

    // can not load resource file
    //    val person_json_path: String = Thread.currentThread().getContextClassLoader.getResource("data/person.json").getPath
    //    val df = spark.read.json(person_json_path)
    //    df.printSchema()
    //    df.show()
    //    println(df.head(2).apply(0).getAs[String]("name"))
    //    df.as[Person](org.apache.spark.sql.Encoders.kryo[Person]).head().age

    //    val row1: Row = new GenericRow(Array(1, "dw", 33, "male")).asInstanceOf[Row]
    //    val row2: Row = new GenericRow(Array(2, "zs", 30, "female")).asInstanceOf[Row]
    //    val row3: Row = new GenericRow(Array(3, "ll", 31, "female")).asInstanceOf[Row]
    //    // one way 这种makeRDD的方式在spark3.x中会报错，需要添加个依赖来升级jar的版本
    //    val rdd = spark.sparkContext.makeRDD(Seq(row1, row2, row3))
    //    val df = spark.createDataFrame(rdd, StructType(Seq(StructField("id", DataTypes.IntegerType), StructField("name", DataTypes.StringType), StructField("age", DataTypes.IntegerType), StructField("gender", DataTypes.StringType))))
    //    // another way
    ////        val df = spark.createDataFrame(List(row1, row2, row3).asJava, StructType(Seq(StructField("id", DataTypes.IntegerType), StructField("name", DataTypes.StringType), StructField("age", DataTypes.IntegerType), StructField("gender", DataTypes.StringType))))
    //    val df1 = df.select("id", "age").show

  //测试行转列 and 列转行
//    val dataFrame = Seq(
//      Row(1, "Employee_1", "Kafka"),
//      Row(2, "Employee_1", "Kibana"),
//      Row(3, "Employee_1", "Hadoop"),
//      Row(4, "Employee_1", "Hadoop"),
//      Row(4, "Employee_1", "Hadoop"),
//      Row(5, "Employee_2", "Spark"),
//      Row(6, "Employee_2", "Scala"),
//      Row(7, "Employee_2", "SageMaker"),
//      Row(7, "Employee_2", "SageMaker"),
//      Row(8, "Employee_3", "GCP"),
//      Row(9, "Employee_3", "AWS"),
//      Row(10, "Employee_3", "Azure"),
//      Row(11, "Employee_4", null)
//    )
//    val dataFrameSchema = new StructType().add("day", IntegerType).add("name", StringType).add("toolSet", StringType)
//    val array_dataframe = spark.createDataFrame(spark.sparkContext.parallelize(dataFrame), dataFrameSchema)
//    //行转列
//    /*
//    +----------+---------------------------------------+
//    |name      |toolSet                                |
//    +----------+---------------------------------------+
//    |Employee_1|[Kafka, Kibana, Hadoop, Hadoop, Hadoop]|
//    |Employee_2|[Spark, Scala, SageMaker, SageMaker]   |
//    |Employee_3|[GCP, AWS, Azure]                      |
//    |Employee_4|[]                                     |
//    +----------+---------------------------------------+
//     */
//    val collect_list_df = array_dataframe.groupBy("name").agg(collect_list("toolSet").as("toolSet"))
//    //行转列去重
//    /*
//    +----------+-------------------------+
//    |name      |toolSet                  |
//    +----------+-------------------------+
//    |Employee_1|[Kafka, Kibana, Hadoop]  |
//    |Employee_2|[Spark, Scala, SageMaker]|
//    |Employee_3|[GCP, AWS, Azure]        |
//    |Employee_4|[]                       |
//    +----------+-------------------------+
//     */
//    //    val collect_list_df = array_dataframe.groupBy("name").agg(array_distinct(collect_list("toolSet")).as("toolSet")) //array_distinct(collect_list()) 等价于 collect_set
//    //    val collect_list_df = array_dataframe.groupBy("name").agg(collect_set("toolSet").as("toolSet"))
//    collect_list_df.printSchema()
//    collect_list_df.show(false)

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
    //    import spark.implicits._
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
    //    val df1 = spark.read.json(person_json_path)


    //    df.join(df1,Seq("name","name"),"left").show()
    //    df.join(df1,$"name" === $"name","left").show()

  }


}
