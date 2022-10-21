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

  /**
   * 可以看到物理计划中有: PushedFilters: [*IsNotNull(tenant_id), *EqualTo(tenant_id,1)]
   *
   * == Physical Plan ==
      AdaptiveSparkPlan isFinalPlan=false
      +- HashAggregate(keys=[status#15], functions=[count(id#0L)], output=[count(id)#57L])
         +- Exchange hashpartitioning(status#15, 200), ENSURE_REQUIREMENTS, [id=#18]
            +- HashAggregate(keys=[status#15], functions=[partial_count(id#0L)], output=[status#15, count#60L])
               +- Scan JDBCRelation(contact_meta_prop) [numPartitions=1] [id#0L,status#15] PushedAggregates: [], PushedFilters: [*IsNotNull(tenant_id), *EqualTo(tenant_id,1)], PushedGroupby: [], ReadSchema: struct<id:bigint,status:string>
   */
  @Test
  def testPushDownGroupBy():Unit = {
    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)
    val df = spark.read.jdbc(url,"contact_meta_prop",connectionProperties)
    df.createOrReplaceTempView("contact_meta_prop")
    val data = spark.sql("select count(id) from contact_meta_prop where tenant_id = 1 group by status;")
    println(data.queryExecution)
  }

  case class TestVO(id:Int,name:String,name1:String)

  @Test
  def testJoin():Unit = {
    val df1 = spark.createDataFrame(List(TestVO(1,"aa","aa1"),TestVO(2,"bb","bb1")))
    val df2 = spark.createDataFrame(List(TestVO(1,"dd","dd1"),TestVO(3,"cc","cc1")))
    val res = df1.join(df2,df1("id") === df2("id"))
    res.show()
    val arr = res.collect().foreach{ row =>
      println(s"value = ${row.getAs[String]("name1")}")
    }
  }

  @AfterEach
  def stop(): Unit = {
    spark.stop()
  }
}
