package com.yiban.spark.minio.scala.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

class MinioTest {
  var spark: SparkSession = _

  @BeforeEach
  def init():Unit = {
    val logger: Logger = Logger.getLogger("org.apache.spark")
    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = SparkSession.builder()
      .appName("MinioTest")
      .master("local[2]")
      .getOrCreate()

  }

  @AfterEach
  def stop():Unit = {
    spark.stop()
  }

  @Test
  def testMinioToOSS():Unit = {
    val rdd = spark.sparkContext.parallelize(Seq(1,2,3,4))
//    rdd.saveAsTextFile("s3a://oss/test/spark/rdd")
//    rdd.saveAsTextFile("s3a://minio-test/test/spark/rdd")
    rdd.saveAsTextFile("s3a://image-leadswarp/test/spark/rdd")
  }
}
