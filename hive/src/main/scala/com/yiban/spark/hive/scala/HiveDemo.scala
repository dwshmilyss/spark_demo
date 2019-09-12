package com.yiban.spark.hive.scala

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object HiveDemo {
  val logger: Logger = Logger.getLogger("org.apache.spark")
  Logger.getLogger("org").setLevel(Level.ERROR)

  // warehouseLocation points to the default location for managed databases and tables
  val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  def main(args: Array[String]): Unit = {
    test1()
  }

  val spark = SparkSession.builder()
    .appName("SparkSQLDemo")
    .master("local[*]")
    //如果不设置该参数 则spark sql会在默认路径创建spark-warehouse
    //    .config("spark.sql.warehouse.dir", warehouseLocation)
    .config("spark.sql.warehouse.dir", "hdfs://master01:9000/user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()

  import spark.sql

  case class Record(key: Int, value: String)

  val kv1_txt_path: String = HiveDemo.getClass.getClassLoader.getResource("data/kv1.txt").getPath

  def test1() = {
    //    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    //    sql("LOAD DATA LOCAL INPATH '" + kv1_txt_path + "' INTO TABLE src")
    //    sql("SELECT * FROM src").show()

    //连接远程的hive时候一定要把core-site.xml hdfs-site.xml hive-site.xml拷贝到resouorces目录
    sql("show databases").collect().foreach(println)
    sql("use test")
    sql("SELECT * FROM testa").show()
  }
}
