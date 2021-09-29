package com.yiban.spark.sql.test

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import java.io.File

class HiveTest {
  val warehouseLocation = new File("spark-warehouse").getAbsolutePath
  println(s"warehouseLocation = $warehouseLocation")

  var spark: SparkSession = _

  @BeforeEach
  def init(): Unit = {
    val logger: Logger = Logger.getLogger("org.apache.spark")
    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = SparkSession.builder()
      .appName("HiveTest")
      .master("local[2]")
      //        .config("spark.sql.warehouse.dir", s"hdfs://hdp1-test.leadswarp.com:8020/apps/hive/warehouse")
      .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .enableHiveSupport()
      .getOrCreate()
  }

  @Test
  def select(): Unit = {
    spark.sql("select * from hive_table").show()
  }

  @Test
  def createHiveTable(): Unit = {
    spark.sql("create table hive_table(id int,name string)")
  }

  @Test
  def insert(): Unit = {
    spark.sql("insert into hive_table values (1,'aa'),(2,'bb'),(3,'bb');")
  }

  @AfterEach
  def stop(): Unit = {
    spark.stop()
  }
}
