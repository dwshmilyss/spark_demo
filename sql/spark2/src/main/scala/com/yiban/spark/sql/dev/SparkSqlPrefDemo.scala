package com.yiban.spark.sql.dev

import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * 这个代码要在spark集群中执行
  *
  * 1、 通过 tpcds-kit 生成 TPC-DS 数据。
  * sudo yum install gcc make flex bison byacc git
  * git clone https://github.com/databricks/tpcds-kit.git
  * cd tpcds-kit/tools
  * make OS=LINUX
  *
  * 2、spark-sql-perf
  * git clone https://github.com/databricks/spark-sql-perf.git
  * sbt package
  * 如果已经有该jar包，则可以省略这步
  *
  * 3、启动 spark-shell
  *
  * 4、生成数据
  * 需要提前将 tpcds-kit 分发到所有 spark executor 节点，注意所有节点的路径要一致
  * 然后执行下面main函数的代码
  *
  * 5、需要导入如下两个jar
  * --jars scala-logging-slf4j_2.11-2.1.2.jar,scala-logging-api_2.11-2.1.2.jar
  */
object SparkSqlPrefDemo {

  val url = "jdbc:mysql://10.21.3.120:3306/test?user=root&password=wenha0"

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
//    .config("spark.some.config.option", "some-value")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]) {
    // 1. 生成数据
//    createData()
    // 2. 执行查询
      executeSelect()
  }

  /**
    * 生成数据
    * 打包后在集群中执行
    * spark-submit --driver-cores 6 --executor-memory 2g --total-executor-cores 28 --executor-cores 1 --conf "spark.memory.fraction=0.8" --conf "spark.memory.storageFraction=0.3" --conf "spark.executor.extraJavaOptions=-XX:+UseConcMarkSweepGC -XX:ParallelGCThreads=16 -XX:ConcGCThreads=16 -XX:CMSInitiatingOccupancyFraction=75 -XX:+UseCMSInitiatingOccupancyOnly -XX:NewRatio=1 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -Xloggc:/usr/local/logs/gc/spark220_282/executor_cms_gc" --class com.yiban.spark.sql.dev.SparkSqlPrefDemo --master spark://10.21.3.73:7077 "/root/jar/sql.jar"
    */
  def createData() = {
    val rootDir = "hdfs://gagcluster/tpcds/data" // root directory of location to create data in.
    val databaseName = "tpcds" // name of database to create.
    val scaleFactor = "20" // scaleFactor defines the size of the dataset to generate (in GB).
    val format = "parquet" // valid spark format like parquet "parquet".
    val dsdgenDir = "/usr/local/src/tpcds-kit/tools"// location of dsdgen

    val sqlContext = spark.sqlContext
    val tables = new TPCDSTables(sqlContext,
      dsdgenDir = dsdgenDir,
      scaleFactor = scaleFactor,
      useDoubleForDecimal = true, // true to replace DecimalType with DoubleType
      useStringForDate = true) // true to replace DateType with StringType


    tables.genData(
      location = rootDir,
      format = format,
      overwrite = true, // overwrite the data that is already there
      partitionTables = true, // create the partitioned fact tables
      clusterByPartitionColumns = true, // shuffle to get partitions coalesced into single files.
      filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
      tableFilter = "", // "" means generate all tables
      numPartitions = 100) // how many dsdgen partitions to run - number of input tasks.

    //    sql(s"create database $databaseName")

    //    tables.createExternalTables(rootDir, format, databaseName, overwrite = true, discoverPartitions = true)
    //创建临时表
    tables.createTemporaryTables(rootDir, format)
  }


  /**
    * 执行查询
    * 在集群中运行
    */
  def executeSelect() = {
    import com.databricks.spark.sql.perf.tpcds.TPCDS
    val sqlContext = spark.sqlContext
    val tpcds = new TPCDS (sqlContext)
    val databaseName = "tpcds"
    println(s"use $databaseName")

    val resultLocation = "hdfs://gagcluster/tpcds/result"
    val iterations = 1
    val queries = tpcds.tpcds2_4Queries
    //单个查询设置超时时间
    val timeout = 300

    val experiment = tpcds.runExperiment(
      queries,
      iterations = iterations,
      resultLocation = resultLocation,
      forkThread = true)
    experiment.waitForFinish(timeout)

//    displayHTML(experiment.html)

    //获取查询结果
    // 1. 如果 experiment 还没有关闭，可以使用 experiment.getCurrentResults 方法从 experiment 获取结果
    import org.apache.spark.sql.functions.{col, lit, substring}
    experiment.getCurrentResults.
      withColumn("Name", substring(col("name"), 2, 100)).
      withColumn("Runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0).
      selectExpr("Name", "Runtime")

    //2. 如果已经关闭，则可以从 resultLocation 中获取结果JSON文件并解析
    val result = spark.read.json(resultLocation)
    import spark.implicits._
    result.select("results.name","results.executionTime").flatMap(r=>{
      val name = r.getAs[Seq[String]]("name")
      val executionTime = r.getAs[Seq[Double]]("executionTime")
      name.zip(executionTime)
    }).toDF("name","executionTime").show()
  }

  /**
    * 或者在spark-shell中执行
    * 在spark-shell中运行测试
    * // 创建sqlContext
    * val sqlContext=new org.apache.spark.sql.SQLContext(sc)
    * import sqlContext.implicits._
    * // 生成数据 参数1：sqlContext  参数2：tpcds-kit目录  参数3：生成的数据量（GB）
    * val tables=new Tables(sqlCotext,"/目录/tpcds-kit/tools",1)
    * tables.genData("hdfs://master:8020:tpctest","parquet",true,false,false,false,false);
    * // 创建表结构（外部表或者临时表）
    * // talbles.createExternalTables("hdfs://master:8020:tpctest","parquet","mytest",false)
    * talbles.createTemporaryTables("hdfs://master:8020:tpctest","parquet")
    * import com.databricks.spark.sql.perf.tpcds.TPCDS
    * val tpcds=new TPCDS(sqlContext=sqlContext)
    * //运行测试
    * val experiment=tpcds.runExperiment(tpcds.tpcds1_4Queries)
    * // 结果保存在 /spark/sql/performance/
    */
}
