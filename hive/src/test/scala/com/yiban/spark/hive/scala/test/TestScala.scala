package com.yiban.spark.hive.scala.test

import com.esotericsoftware.kryo.{Kryo, Registration}
import com.esotericsoftware.kryo.Kryo.DefaultInstantiatorStrategy
import com.esotericsoftware.kryo.io.{Input, Output}
import com.twitter.chill.EnumerationSerializer
import com.yiban.spark.hive.scala.utils.FSUtils
import org.apache.avro.Schema
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.common.model.{OverwriteNonPartialWithLatestAvroPayload, WriteOperationType}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
import org.json.Test
import org.objenesis.strategy.StdInstantiatorStrategy

import java.io.{ByteArrayInputStream, FileInputStream, FileOutputStream, IOException, ObjectInputStream, ObjectOutputStream}
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement}
import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneId}
import java.util
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.JavaConverters._
import org.junit.jupiter.api.{BeforeEach, Test}

class TestScala extends Serializable {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("io").setLevel(Level.ERROR)
  private var spark: SparkSession = _

  case class Test1(id: Int, ts: Int, name1: String, action: String, name3: String, name4: String,date:String)

  @BeforeEach
  def init: Unit = {
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.catalogImplementation", "hive")
    //    System.setProperty("hadoop.home.dir", "/Users/duanwei/apps/hadoop-2.7.3")
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    spark = SparkSession.builder()
      .appName("cronjob")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", s"hdfs://hdp1-test.leadswarp.com:8020/apps/hive/warehouse")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "512m")
      .enableHiveSupport()
      .getOrCreate()
  }

  def setWriteOptions(path: String): Map[String, String] = {
    Map(
      "path" -> path
    )
  }

  /**
   * Acquisition of today's zero hour demo : 2020-11-27 00:00:00  1606406400000
   */
  def setTime(): Long = {
    LocalDateTime
      .of(LocalDate.now(), LocalTime.MIN).atZone(ZoneId.systemDefault())
      .toInstant.toEpochMilli
  }





  @Test
  def test(): Unit = {
    //    val list = List("1","2","3")
    //    val seq = Seq("1","2","3")
    //    val arr = Array("1","2","3")
    //    println(SizeEstimator.estimate(list))
    //    println(SizeEstimator.estimate(arr))
    //    println(SizeEstimator.estimate(seq))
    //    val path = this.getClass.getClassLoader.getResource("person.json").getPath
    val path = this.getClass.getClassLoader.getResource("person.csv").getPath
    println(path)
    val ds = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(path)
    ds.printSchema()
    ds.show()
    println(ds.schema)
    println(ds.schema.fields(0).name)

    spark.sql("show databases").show()
    spark.sql("show tables").show()
    spark.sql("select * from person").show()
//      ds.write
//      .mode(SaveMode.Overwrite)
//      .option("path" , "hdfs://hdp1-test.leadswarp.com:8020/hive/tables")
//      .saveAsTable(s"person")
    //    val aliasArr: List[(String, String)] = Set("group_3608", "group_3626","group_3642").map((_, "String")).toList
    //    updateHasData(aliasArr)

  }




  /**
   * Get the MySQL connection
   */
  def getConnection(databaseAndTableName: String): Connection = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection(
      "jdbc:mysql://192.168.26.11:30053/linkflow?useUnicode=yes&characterEncoding=UTF-8&useLegacyDatetimeCode=false&serverTimezone=GMT%2b8&zeroDateTimeBehavior=convertToNull&useSSL=false&cachePrepStmts=true&prepStmtCacheSize=250&prepStmtCacheSqlLimit=2048&useServerPrepStmts=true&useLocalSessionState=true&useLocalTransactionState=true&rewriteBatchedStatements=true&cacheResultSetMetadata=true&cacheServerConfiguration=true&elideSetAutoCommits=true&maintainTimeStats=false",
      "root",
      "Sjtu403c@#%")
  }

  /**
   * A single Custom data insert into MySQL
   *
   * @param dataAndTypes List((1,"Int"))
   */
  def writeMysql(dataAndTypes: List[(Any, String)], tableName: String, getSql: String): Unit = {
    var connection: Connection = null
    var stat: PreparedStatement = null
    try {
      connection = getConnection(tableName)
      connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)

      val sql = getSql

      stat = connection.prepareStatement(sql)
      connection.setAutoCommit(true)

      for (i <- 0 to dataAndTypes.size - 1) {
        makeSetter(i, stat, dataAndTypes(i), connection)
      }

      stat.execute()

    } catch {
      case exception: Exception => throw new Exception(exception)
    } finally {
      if (stat != null) stat.close()
      if (connection != null) connection.close()
    }
  }

  /**
   * A single entry is inserted into MySQL conversion logic
   *
   * @param dataAndType (1,"Int")
   */
  def makeSetter(pos: Int, stat: PreparedStatement, dataAndType: (Any, String), connection: Connection): Unit = dataAndType._2 match {
    case "Int" => stat.setInt(pos + 1, dataAndType._1.asInstanceOf[Int])
    case "Long" => stat.setLong(pos + 1, dataAndType._1.asInstanceOf[Long])
    case "Double" => stat.setDouble(pos + 1, dataAndType._1.asInstanceOf[Double])
    case "Boolean" => stat.setBoolean(pos + 1, dataAndType._1.asInstanceOf[Boolean])
    case "String" => stat.setString(pos + 1, dataAndType._1.asInstanceOf[String])
    case "Timestamp" => stat.setTimestamp(pos + 1, dataAndType._1.asInstanceOf[java.sql.Timestamp])
    case "BigDecimal" => stat.setBigDecimal(pos + 1, dataAndType._1.asInstanceOf[java.math.BigDecimal])
    case "Float" => stat.setFloat(pos + 1, dataAndType._1.asInstanceOf[Float])
    case "Short" => stat.setShort(pos + 1, dataAndType._1.asInstanceOf[Short])
    case "Byte" => stat.setByte(pos + 1, dataAndType._1.asInstanceOf[Byte])
    case "Date" => stat.setDate(pos + 1, dataAndType._1.asInstanceOf[java.sql.Date])
    case _ => throw new IllegalArgumentException(s"There is no matching type ${dataAndType._2} data ${dataAndType._1}")
  }

  /**
   * DataFrame Save to a specific implementation of MySQL
   */
  def writeMysql(iter: Iterator[Row], tableName: String, getSql: String): Unit = {
    var connection: Connection = null
    var stat: PreparedStatement = null
    try {
      connection = getConnection(tableName)
      connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)

      val sql = getSql

      var count = 0
      var flag = true
      var nameAndTypes: Array[StructField] = null

      stat = connection.prepareStatement(sql)
      connection.setAutoCommit(true)

      iter.foreach(row => {
        if (flag) {
          nameAndTypes = row.schema.fields
          flag = false
        }
        for (i <- 0 to nameAndTypes.size - 1) {
          makeSetter(i, stat, row, nameAndTypes(i))
        }
        stat.addBatch()
        count += 1
        if (count == 5000) {
          stat.executeBatch()
          stat.clearBatch()
          count = 0
        }
      })
      stat.executeBatch()

    } catch {
      case exception: Exception => throw new Exception(exception)
    } finally {
      if (stat != null) stat.close()
      if (connection != null) connection.close()
    }
  }

  /**
   * Multiple pieces of conversion logic are inserted into MySQL
   */
  def makeSetter(pos: Int, stat: PreparedStatement, row: Row, nameAndType: StructField): Unit = nameAndType.dataType match {
    case IntegerType => stat.setInt(pos + 1, row.getAs[Int](pos))
    case LongType => stat.setLong(pos + 1, row.getAs[Long](pos))
    case DoubleType => stat.setDouble(pos + 1, row.getAs[Double](pos))
    case BooleanType => stat.setBoolean(pos + 1, row.getAs[Boolean](pos))
    case StringType => stat.setString(pos + 1, row.getAs[String](pos))
    case TimestampType => stat.setTimestamp(pos + 1, row.getAs[java.sql.Timestamp](pos))
    case t: DecimalType => stat.setBigDecimal(pos + 1, row.getAs[java.math.BigDecimal](pos))
    case FloatType => stat.setFloat(pos + 1, row.getAs[Float](pos))
    case ShortType => stat.setShort(pos + 1, row.getAs[Short](pos))
    case ByteType => stat.setByte(pos + 1, row.getAs[Byte](pos))
    case DateType => stat.setDate(pos + 1, row.getAs[java.sql.Date](pos))
    case _ => throw new IllegalArgumentException(s"There is no matching type ${nameAndType.dataType} name ${nameAndType.name}")
  }


}

class Student() {
  var id: Int = 0
  var name: String = "dw"

  def this(id: Int, name: String) {
    this()
    this.id = id
    this.name = name
  }
}
