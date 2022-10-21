package com.yiban.spark.hudi.scala.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

class HudiTest {

  var spark: SparkSession = _

  @BeforeEach
  def init():Unit = {
    val logger: Logger = Logger.getLogger("org.apache.spark")
    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = SparkSession.builder()
      .appName("HudiTest")
      .master("local[2]")
      //        .config("spark.sql.warehouse.dir", s"hdfs://hdp1-test.leadswarp.com:8020/apps/hive/warehouse")
      //    .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
//      .enableHiveSupport()
      .getOrCreate()
  }

  @Test
  def testCreateHudiTable(): Unit = {
    spark.sql("create table if not exists hudi_table (id int,ts int,name string,date string) using hudi options (type = 'cow',primaryKey = 'id',preCombineField = 'ts',hoodie.sql.origin.keygen.class='org.apache.hudi.keygen.SimpleKeyGenerator') partitioned by (date)")
    spark.sql("show tables").show()
  }

  @Test
  def testInsertHudiTable(): Unit = {
    spark.sql("insert into hudi_test_partition values (1,1,'aa','2015-01-01'),(2,1,'bb','2015-01-01'),(3,2,'cc','2015-01-02'),(4,2,'dd','2015-01-02')")
    //    spark.sql("insert into hudi_table partition(date='2015-01-01') select 1,1,'aa';")
    spark.sql("select * from hudi_test_partition").show()
  }

  @Test
  def testPushDownGroupBy(): Unit = {
    val df = spark.sql("select count(1) from hudi_table where ts>1 group by date")
    println(df.queryExecution)
  }

  @Test
  def testParquet(): Unit = {
    val df1 = spark.read.format("parquet").load("hdfs://hdp1-pre1.leadswarp.com:8020/hudi/tenant362/contact/default/94786088-8182-4576-9a00-b0c0e8ec8b28-0_0-343-18566_20220106155507.parquet")
    df1.createOrReplaceTempView("test1")
//    println(df1.count())
    spark.sql("select * from test1 order by id limit 10").show()
    val df2 = spark.read.format("parquet").load("hdfs://hdp1-pre1.leadswarp.com:8020/hudi/tenant362/contact/default/94786088-8182-4576-9a00-b0c0e8ec8b28-1_0-447-23877_20220106160029.parquet")
    df2.createOrReplaceTempView("test2")
//    println(df2.count())
    spark.sql("select * from test2 order by id limit 10").show()

  }

  @AfterEach
  def stop():Unit = {
    spark.stop()
  }
}
