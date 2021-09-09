package com.yiban.spark.ignite.scala

import org.apache.ignite.spark.IgniteDataFrameSettings.{FORMAT_IGNITE, OPTION_CONFIG_FILE, OPTION_SCHEMA, OPTION_TABLE}
import org.apache.spark.sql.{SaveMode, SparkSession}

object ScalaDemo {

  case class Person3(id:Int,name:String)

  def main(args: Array[String]): Unit = {
//        testJDBC()
    testAPI()
//    testWriteTable()
  }

  def testJDBC() = {
    val spark: SparkSession = SparkSession.builder()
      .appName("ignite")
//      .master("local[2]")
      .config("spark.executor.instances", "1")
      .getOrCreate()

    val df = spark.read
      .format("jdbc")
      .option("url", "jdbc:ignite:thin://10.106.1.16")
      .option("fetchsize", 100)
      //.option("driver", "org.apache.ignite.IgniteJdbcDriver")
      .option("dbtable", "person")
      .load()

    df.printSchema()

    df.createOrReplaceTempView("person")

    val igniteDF = spark.sql("select * from person where id=3")
    igniteDF.show()
  }

  def testAPI() = {
    val spark: SparkSession = SparkSession.builder()
      .appName("ignite")
      .master("local[2]")
      .config("spark.executor.instances", "1")
      .getOrCreate()
    //        val cfgPath: String = ScalaDemo.getClass.getClassLoader.getResource("local.xml").getPath
    //        println(s"cfgPath = $cfgPath")
    //
    //        val df = spark.read
    //          .format(FORMAT_IGNITE)               // Data source type.
    //          .option(OPTION_TABLE, "person")      // Table to read.
    //          .option(OPTION_CONFIG_FILE, cfgPath) // Ignite config.
    //          .load()

    //        val hdfsPath = "hdfs://localhost:9000/local.xml"
    val hdfsPath = "/Users/edz/sourceCode/spark_demo/ignite/src/main/resources/local.xml"
//        val hdfsPath = "/Users/edz/apps/apache-ignite-2.9.1-bin/config/default-config.xml"
    val df = spark.read
      .format(FORMAT_IGNITE)
      .option(OPTION_TABLE, "person")
      .option(OPTION_SCHEMA, "public")
      .option(OPTION_CONFIG_FILE, hdfsPath)
      .load()
    df.printSchema()
//    df.show()
    df.createOrReplaceTempView("person")

    val igniteDF = spark.sql("select * from person where id=3")
    igniteDF.show()
    spark.stop()
  }

  def testWriteTable() = {
    val spark: SparkSession = SparkSession.builder()
      .appName("ignite")
      .master("local[2]")
      .config("spark.executor.instances", "1")
      .getOrCreate()

    val hdfsPath = "/Users/edz/sourceCode/spark_demo/ignite/src/main/resources/local.xml"
    val rdd = spark.sparkContext.makeRDD(List(Person3(1,"aa"),Person3(2,"bb")))
    val df = spark.createDataFrame(rdd)
    df.write.format("ignite").option("config",hdfsPath).option("table","person4").option("streamerAllowOverwrite","true").option("primaryKeyFields","id").mode(SaveMode.Overwrite).save()
    spark.stop()
  }
}
