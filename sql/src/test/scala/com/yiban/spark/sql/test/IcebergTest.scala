package com.yiban.spark.sql.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import java.sql.Date

class IcebergTest {

  var spark: SparkSession = _
  var sparkWithHive: SparkSession = _
  case class Person(id:Int,name:String,age:Int,sex:String)
  case class Person1(id:Int,name:String,age:Int,sex:String,birthday:Date)
  case class Person2(id:Int,name:String,age:Int,sex:String,birthday:String)
  var storeDir = "/iceberg"

  @BeforeEach
  def init() = {
    val logger: Logger = Logger.getLogger("org.apache.spark")
    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = SparkSession.builder()
      .appName("IcebergTest")
      .master("local[2]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hadoop")
      .config("spark.sql.catalog.spark_catalog.warehouse", storeDir)
      .getOrCreate()

    //spark.sql.catalog.hive_prod.uri=thrift://localhost:9083
    sparkWithHive = SparkSession.builder()
      .appName("IcebergWithHiveTest")
      .master("local[2]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      .config("spark.sql.catalog.spark_catalog.warehouse", "hdfs://localhost:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()
  }

  @Test
  def testCreate(): Unit = {
    spark.sql("CREATE TABLE test (id BIGINT NOT NULL, data STRING) USING iceberg")
    spark.sql("select * from test").show()
  }

  @Test
  def testSelect(): Unit = {
    spark.sql("select * from spark_catalog.default.test").show()
  }

  @Test
  def testInsert(): Unit = {
    spark.sql("insert into spark_catalog.default.test values (1,'aa'),(2,'bb')")
    spark.sql("select * from spark_catalog.default.test").show()
  }

  @Test
  def testUpdate(): Unit = {
    //does not support
    spark.sql("update spark_catalog.default.test set data = 'aaa' where id=1;")
    spark.sql("select * from spark_catalog.default.test").show()
  }

  @Test
  def testMergeInto(): Unit = {
    spark.sql("merge into spark_catalog.default.test t using (select 1 as id,'aa' as data) t1 on t.id=t1.id when matched then update set t.data = t1.data when not matched then insert *;")
    spark.sql("select * from spark_catalog.default.test").show()
  }

  @Test
  def testDelete(): Unit = {
    spark.sql("delete from  spark_catalog.default.test where id=1;")
    spark.sql("select * from spark_catalog.default.test").show()
  }

  @Test
  def testCreatePersonTable(): Unit = {
    spark.sql("CREATE TABLE spark_catalog.default.person (id int,name STRING,age int,sex string) USING iceberg")
  }

  @Test
  def testInsertAPI():Unit = {
    val personDF = spark.createDataFrame(Seq(Person(1, "aa", 18, "male"), Person(2, "bb", 20, "female"), Person(3, "cc", 21, "female")))
//    personDF.writeTo("spark_catalog.default.person").append()
    spark.sql("select * from spark_catalog.default.person;").show()
  }

  @Test
  def testAPI():Unit = {
    val personDF = spark.createDataFrame(Seq(Person1(1, "eee", 30, "male",new Date(System.currentTimeMillis()))))
//    personDF.writeTo("spark_catalog.default.person").append()
//    personDF.writeTo("spark_catalog.default.person").replace()
//    personDF.writeTo("spark_catalog.default.person").overwritePartitions()
    spark.sql("select * from spark_catalog.default.person;").show()
  }

  @Test
  def testAlterTable():Unit = {
    //no viable(不可行)
    spark.sql("alter table spark_catalog.default.person add COLUMNS (birthday date)")
  }

  @Test
  def test():Unit = {
    spark.sql("select * from spark_catalog.default.person;").show()
//    spark.sql("show tables in default;").show()
  }

  @Test
  def testCreateTableWithHive():Unit = {
    sparkWithHive.sql("CREATE TABLE spark_catalog.default.iceberg_person (id int,name STRING,age int,sex string,birthday string) USING iceberg PARTITIONED BY (bucket(2, id), birthday, sex)")
  }

  @Test
  def testInsertWithHive(): Unit = {
    //dynamic partition
    spark.sql("insert into spark_catalog.default.iceberg_person values (1,'aa',18,'male','2020-01-01'),(2,'bb',20,'female','2020-01-02')")
    //static partition
//    spark.sql("insert into spark_catalog.default.iceberg_person partition(birthday='2020-01-01',sex='male') values (3,'cc',21),(4,'dd',22);")
    //dynamic+static
//    spark.sql("insert into spark_catalog.default.iceberg_person partition(birthday='2020-01-02') values (6,'ee',31,'male'),(5,'ff',32,'female');")

    spark.sql("select * from spark_catalog.default.iceberg_person").show()
  }

  @AfterEach
  def stop() = {
    spark.stop()
    sparkWithHive.stop()
  }
}
