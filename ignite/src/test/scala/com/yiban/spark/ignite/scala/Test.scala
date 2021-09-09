package com.yiban.spark.ignite.scala


import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.spark.IgniteContext
import org.apache.ignite.spark.IgniteDataFrameSettings.{FORMAT_IGNITE, OPTION_CONFIG_FILE, OPTION_TABLE}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

object Test {
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
  def test() = {
    val cfgPath: String = Test.getClass.getClassLoader.getResource("local.xml").getPath
    println(cfgPath)

    val df = spark.read
      .format(FORMAT_IGNITE) // Data source type.
      .option(OPTION_TABLE, "person") // Table to read.
      .option(OPTION_CONFIG_FILE, cfgPath) // Ignite config.
      .load()

    df.createOrReplaceTempView("person")

    val igniteDF = spark.sql("select * from person where id=3")
    igniteDF.show()
  }

  @Test
  def testJDBC() = {
    val df = spark.read
      .format("jdbc")
      .option("url", "jdbc:ignite:thin://10.106.1.16")
      .option("fetchsize", 100)
      //.option("driver", "org.apache.ignite.IgniteJdbcDriver")
      .option("dbtable", "person").load()

    df.printSchema()

    df.createOrReplaceTempView("person")

    val igniteDF = spark.sql("select * from person where id=3")
    igniteDF.show()
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
