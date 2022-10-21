package com.yiban.spark.ignite.scala


import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.spark.IgniteContext
import org.apache.ignite.spark.IgniteDataFrameSettings.{FORMAT_IGNITE, OPTION_CONFIG_FILE, OPTION_TABLE}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.springframework.util.StopWatch

import java.net.URL

class IgniteTest {
  var spark: SparkSession = _

  @BeforeEach
  def init() = {
    spark = SparkSession.builder()
      .appName("ignite")
      .master("local")
      .config("spark.executor.instances", "2")
      .getOrCreate()

    //    val igniteContext = new IgniteContext(spark.sparkContext,
    //      () => new IgniteConfiguration())

  }

  @Test
  def testMysql() = {
    val df = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test?user=root&password=120653")
      //.option("driver", "org.apache.ignite.IgniteJdbcDriver")
      .option("dbtable", "tb1").load()

    df.printSchema()

  }

  @Test
  def testAPI() = {
    val cfgPath: String = new IgniteTest().getClass.getClassLoader.getResource("local_static_ip-config.xml").getPath
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())
//    val cfgPath: String = "hdfs://localhost:9000/local_static_ip-config.xml"
    println(cfgPath)

    val df = spark.read
      .format(FORMAT_IGNITE) // Data source type.
      .option(OPTION_TABLE, "city") // Table to read.
      .option(OPTION_CONFIG_FILE, cfgPath) // Ignite config.
      .load()

    df.createOrReplaceTempView("city")
      val stopWatch:StopWatch = new StopWatch()
      stopWatch.start()
//    val igniteDF = spark.sql("select * from identity_test  where id = '1'")
    val igniteDF = spark.sql("select * from city")
    igniteDF.explain(true)
    println(igniteDF.queryExecution.executedPlan)
    igniteDF.show()
    stopWatch.stop()
    println(stopWatch.getTotalTimeMillis)//25105 ,17668
  }

  @Test
  def testJDBC() = {
    val df = spark.read
      .format("jdbc")
      .option("url", "jdbc:ignite:thin://localhost:10800;partitionAwareness=true")
      .option("fetchsize", 100)
      //.option("driver", "org.apache.ignite.IgniteJdbcDriver")
      .option("dbtable", "city").load()

    df.printSchema()

    df.createOrReplaceTempView("city")

    val stopWatch:StopWatch = new StopWatch()
    stopWatch.start()
    //    val igniteDF = spark.sql("select * from identity_test  where id = '1'")
    val igniteDF = spark.sql("select * from city")
    igniteDF.show()
    stopWatch.stop()
    println(stopWatch.getTotalTimeMillis)//31083
  }

  @Test
  def testJDBCAPI() = {
    //    import java.sql.DriverManager
    //    val connection = DriverManager.getConnection("jdbc:ignite:thin://10.106.1.16")

    import java.util.Properties
    val connectionProperties = new Properties()

    connectionProperties.put("url", "jdbc:ignite:thin://10.106.1.16")

    spark.sql("select 1 as ID,'blah' as NAME")
      .write
      .mode(SaveMode.Append)
      .jdbc(connectionProperties.getProperty("url"), "person", connectionProperties)

    spark.read
      .format("jdbc")
      .option("url", "jdbc:ignite:thin://10.106.1.16")
      .option("fetchsize", 100)
      .option("dbtable", "person").load().show(10, false)
  }

  @AfterEach
  def close() = {
    spark.stop()
  }
}
