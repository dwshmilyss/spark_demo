package com.yiban.spark.sql.test

import com.yiban.spark.sql.dev.MySQLDemo.spark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import java.util.Properties

class MySQLTest {
  var spark: SparkSession = _
  val url = "jdbc:mysql://192.168.26.11:30053/linkflow?useUnicode=yes&characterEncoding=UTF-8&useLegacyDatetimeCode=false&serverTimezone=GMT%2b8&zeroDateTimeBehavior=convertToNull&useSSL=false&cachePrepStmts=true&prepStmtCacheSize=250&prepStmtCacheSqlLimit=2048&useServerPrepStmts=true&useLocalSessionState=true&useLocalTransactionState=true&rewriteBatchedStatements=true&cacheResultSetMetadata=true&cacheServerConfiguration=true&elideSetAutoCommits=true&maintainTimeStats=false"
  val driver = "com.mysql.jdbc.Driver"
  val user = "root"
  val password = "Sjtu403c@#%"

  @BeforeEach
  def init(): Unit = {
    val logger: Logger = Logger.getLogger("org.apache.spark")
    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = SparkSession.builder()
      .appName("MySQLTest")
      .master("local[2]")
      .getOrCreate()
  }

  @Test
  def testPushDownGroupBy():Unit = {
//    val connectionProperties = new Properties()
//    connectionProperties.put("user", user)
//    connectionProperties.put("password", password)
//    val df = spark.read.jdbc(url,"contact_meta_prop",connectionProperties)

    val df = spark.read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", "contact_meta_prop")
      .option("user", user)
      .option("password", password)
      .load()


    df.createOrReplaceTempView("contact_meta_prop")
    val data = spark.sql("select count(id) from contact_meta_prop where tenant_id = 1 group by status;")
    println(data.queryExecution)
  }

  @AfterEach
  def stop(): Unit = {
    spark.stop()
  }
}
