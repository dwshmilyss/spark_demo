package com.yiban.spark.sql.dev

import org.apache.spark.sql.SparkSession
import com.databricks.spark.sql.perf.tpcds.TPCDSTables

object SparkSqlPrefDemo {

  val url = "jdbc:mysql://10.21.3.120:3306/test?user=root&password=wenha0"

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]) {
    val rootDir = "tpctest" // root directory of location to create data in.
    val databaseName = "tpctest" // name of database to create.
    val scaleFactor = "1" // scaleFactor defines the size of the dataset to generate (in GB).
    val format = "parquet" // valid spark format like parquet "parquet".

    val sqlContext = spark.sqlContext
    val tables = new TPCDSTables(sqlContext,
      dsdgenDir = "/tmp/tpcds-kit/tools", // location of dsdgen
      scaleFactor = scaleFactor,
      useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
      useStringForDate = false) // true to replace DateType with StringType


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

    tables.createExternalTables(rootDir, "parquet", databaseName, overwrite = true, discoverPartitions = true)

  }
}
