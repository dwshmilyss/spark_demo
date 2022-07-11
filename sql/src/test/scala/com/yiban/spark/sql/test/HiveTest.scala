package com.yiban.spark.sql.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import java.io.File

class HiveTest {
  val warehouseLocation = new File("spark-warehouse").getAbsolutePath
  println(s"warehouseLocation = $warehouseLocation")

  case class Person(id: Int, name: String, age: Int, sex: String)

  var spark: SparkSession = _

  @BeforeEach
  def init(): Unit = {
    val logger: Logger = Logger.getLogger("org.apache.spark")
    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = SparkSession.builder()
      .appName("HiveTest")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", s"hdfs://hdp1-pre1.leadswarp.com:8020/apps/hive/warehouse")
      .config("hive.metastore.uris","thrift://192.168.121.32:9083")
      //      .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
      //      .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
      //      .config("hive.support.concurrency","true")
      //      .config("hive.txn.manager","org.apache.hadoop.hive.ql.lockmgr.DbTxnManager")
      //      .config("hive.enforce.bucketing","true")
      //      .config("hive.exec.dynamic.partition.mode","nostrict")
      //      .config("hive.compactor.initiator.on","true")
      //      .config("hive.compactor.worker.threads","1")
      .enableHiveSupport()
      .getOrCreate()
  }

  /**
   * +--------+
|count(1)|
+--------+
| 2046674|
+--------+
   */
  @Test
  def test(): Unit = {
    spark.sql("select count(1) from dw_t1.contact").show()
  }

  @Test
  def selectWithSql(): Unit = {
    spark.sql("select * from hive_table").show()
  }

  @Test
  def createTableWithSql(): Unit = {
    spark.sql("create table hive_table(id int,name string)")
  }

  @Test
  def insertWithSql(): Unit = {
    spark.sql("insert into hive_table values (1,'aa'),(2,'bb'),(3,'bb');")
    spark.sql("select * from hive_table").show()
  }

  @Test
  def createTableWithAPI(): Unit = {
    val personDF = spark.createDataFrame(Seq(Person(1, "aa", 18, "male"), Person(2, "bb", 20, "female"), Person(3, "cc", 21, "female")))
    personDF.write.format("hive")
      .mode(SaveMode.Overwrite)
      //这里可以不指定path，如果不指定，则会存储在hive默认的warehouse下
      //      .option("path","hdfs://localhost:9000/user/hive/warehouse/hive_person")
      .saveAsTable("person1")
  }


  @Test
  def createOrcTableWithAPI(): Unit = {
    val personDF = spark.createDataFrame(Seq(Person(1, "aa", 18, "male"), Person(2, "bb", 20, "female"), Person(3, "cc", 21, "female")))
    personDF.write.format("orc")
      .partitionBy("sex")
      .bucketBy(8, "age")
      .mode(SaveMode.Overwrite)
      //      .option("transactional","true")
      //这里可以不指定path，如果不指定，则会存储在hive默认的warehouse下
      //      .option("path","hdfs://localhost:9000/user/hive/warehouse/hive_person")
      .saveAsTable("person_orc_acid")
  }

  @Test
  def createOrcTableWithHQL(): Unit = {
    val personDF = spark.createDataFrame(Seq(Person(1, "aa", 18, "male"), Person(2, "bb", 20, "female"), Person(3, "cc", 21, "female")))

    //如果要创建可以update的orc表，必须用原生的HQL语法
    //    println(spark.sql("create table person_orc_bucket_v2(id int,name string,age int) partitioned by (sex string) CLUSTERED BY (age) INTO 8 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true');")
    //      .queryExecution)

    //spark sql不支持创建hive的clustered by bucket
    println(spark.sql("create table person_orc_bucket_v2(id int,name string,age int,sex string) using hive partitioned by (sex) options(fileFormat 'orc');").queryExecution)

  }


  @Test
  def createOrcTableWithV2API(): Unit = {
    val personDF = spark.createDataFrame(Seq(Person(1, "aa", 18, "male"), Person(2, "bb", 20, "female"), Person(3, "cc", 21, "female")))
//    personDF.writeTo("person_orc_bucket_v2")
//      .partitionedBy(col("sex"))
//      .tableProperty("transactional", "true")
//      .create()

  }

  @Test
  def testRead(): Unit = {
    val personDF = spark.createDataFrame(Seq(Person(1, "aa", 18, "male"), Person(2, "bb", 20, "female"), Person(3, "cc", 21, "female")))
  }


  @Test
  def InsertOrcTableWithAPI(): Unit = {
    val personDF = spark.createDataFrame(Seq(Person(4, "aa", 18, "male"), Person(5, "bb", 20, "female"), Person(6, "cc", 21, "female")))
    personDF.write.format("orc")
      .mode(SaveMode.Append)
      //这里可以不指定path，如果不指定，则会存储在hive默认的warehouse下
      //      .option("path","hdfs://localhost:9000/user/hive/warehouse/hive_person")
      .insertInto("person_orc_bucket")
  }

  @AfterEach
  def stop(): Unit = {
    spark.stop()
  }
}
