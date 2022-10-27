package com.yiban.spark31.sql.test

import com.yiban.spark31.sql.test.entity.{Person, Salary}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, dense_rank, rank, row_number}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{Encoders, SparkSession, functions}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import java.util.Properties
import scala.jdk.CollectionConverters.IterableHasAsJava
import scala.language.postfixOps

class PushDownTest {
  val url = "jdbc:mysql://192.168.26.11:30053/linkflow?useUnicode=yes&characterEncoding=UTF-8&useLegacyDatetimeCode=false&serverTimezone=GMT%2b8&zeroDateTimeBehavior=convertToNull&useSSL=false&cachePrepStmts=true&prepStmtCacheSize=250&prepStmtCacheSqlLimit=2048&useServerPrepStmts=true&useLocalSessionState=true&useLocalTransactionState=true&rewriteBatchedStatements=true&cacheResultSetMetadata=true&cacheServerConfiguration=true&elideSetAutoCommits=true&maintainTimeStats=false"
  val driver = "com.mysql.jdbc.Driver"
  val user = "root"
  val password = "Sjtu403c@#%"

  @BeforeEach
  def init(): Unit = {
    val logger: Logger = Logger.getLogger("org.apache.spark")
    Logger.getLogger("org").setLevel(Level.ERROR)
  }

  /**
   * df.filter('i > 3).groupBy('j).agg(sum($"i")).where(sum('i) > 10)
   *
   * After Aggregate Push Down, it has the following Optimized Logical Plan and Physical Plan:
   *
   * == Optimized Logical Plan ==
    DataSourceV2Relation (source=AdvancedDataSourceV2,schema=[i#0 int, j#1 int],filters=[isnotnull(i#0), (i#0 > 3)] aggregates=[sum(cast(i#0 as bigint))], groupby=[j#1], havingClause==[sum(cast(i#0 as bigint))>10], options=Map())

    == Physical Plan ==
    DataSourceV2Scan(source=AdvancedDataSourceV2,schema=[i#0 int, j#1 int],filters=[isnotnull(i#0), (i#0 > 3)] aggregates=[sum(cast(i#0 as bigint))], groupby=[j#1], havingClause==[sum(cast(i#0 as bigint))>10], options=Map())
   */
  @Test
  def testPushDownGroupBy():Unit = {
   val spark = SparkSession.builder()
      .appName("MySQLTest")
      .master("local[2]")
      .getOrCreate()
    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)
    val df = spark.read.jdbc(url,"contact_meta_prop",connectionProperties)
    df.createOrReplaceTempView("contact_meta_prop")
//    val data = spark.sql("select status,count(id) as cn from contact_meta_prop where tenant_id = 1 group by status having cn = 10;")
    df.filter(col("tenant_id") === 1).groupBy(col("status")).agg(count(col("id"))).where(count(col("id")) > 10).explain(true)
//    data.queryExecution.debug
//    println(s"logical = ${spark.sessionState.analyzer.ResolveRelations(data.queryExecution.logical)}")
//    println(data.queryExecution)
  }


  @Test
  def testPushDownProjection():Unit = {
    val sparkSession = SparkSession.builder()
    .appName("testPushDownProjection")
    .master("local[2]")
    .getOrCreate()
    import sparkSession.implicits._
    val ds2 = Seq(Person("Michael", 29), Person("Andy", 30), Person("Justin", 11)).toDS()
    val ds3 = Seq(Person("Michael", 9), Person("Andy", 7), Person("Justin", 11)).toDS()

    // 进行数据集的union和select运算
    ds2.union(ds3).union(ds2).select("name").explain(true)
  }

  @Test
  def testPushDownWindow():Unit = {
    val sparkSession = SparkSession.builder()
      .appName("testPushDownProjection")
      .master("local[2]")
      .getOrCreate()
    import sparkSession.implicits._
    val empSalay = Seq(
      Salary("sales",1,5000),
      Salary("sales",12,3000),
      Salary("personnel",2,3000),
      Salary("sales",3,4800),
      Salary("sales",4,4800),
      Salary("personnel",5,3500),
      Salary("develop",6,4200),
      Salary("develop",7,4200),
      Salary("develop",8,5600),
      Salary("develop",9,6000),
      Salary("develop",10,7700),
      Salary("develop",11,8800)
    ).toDS()

    val byDepSalaryDesc = Window.partitionBy('dep).orderBy('salary desc)
    val rankByDep = rank().over(byDepSalaryDesc)
//    val rankByDep = dense_rank().over(byDepSalaryDesc)
//    val rankByDep = row_number().over(byDepSalaryDesc)
    empSalay.select('*,rankByDep as 'rank).where(col("dep") === ("develop")).explain(true)
//    empSalay.select('*,rankByDep as 'rank).where(col("dep") === ("develop")).show()

  }

  /**
   * limit这种就不能下推，因为下推了再过滤就不对了，例如ds2
   */
  @Test
  def testPushDownLimit():Unit = {
    val spark = SparkSession.builder()
      .appName("testPushDownProjection")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val ds1 = spark.createDataset(1 to 100).limit(10).filter('value % 10 === 0)
    val ds2 = spark.createDataset(1 to 100).filter('value % 10 === 0).limit(10)
    ds1.collect().foreach(println)
    ds1.explain(true)
    ds2.collect().foreach(println)
    ds2.explain(true)
  }

  @Test
  def testPushDownGroupBy1():Unit = {
    val spark = SparkSession.builder()
      .appName("MySQLTest")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val empSalay = Seq(
      Salary("sales",1,5000),
      Salary("sales",12,3000),
      Salary("personnel",2,3000),
      Salary("sales",3,4800),
      Salary("sales",4,4800),
      Salary("personnel",5,3500),
      Salary("develop",6,4200),
      Salary("develop",7,4200),
      Salary("develop",8,5600),
      Salary("develop",9,6000),
      Salary("develop",10,7700),
      Salary("develop",11,8800)
    ).toDS()

    val df =  empSalay.filter(col("dep") === "develop").groupBy(col("dep")).agg(functions.max("salary")).where(functions.max("salary") > 8000)
    df.explain(true)
  }

}
