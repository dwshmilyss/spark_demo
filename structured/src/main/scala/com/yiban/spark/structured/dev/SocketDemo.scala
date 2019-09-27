package com.yiban.spark.structured.dev

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SocketDemo {

  val logger = Logger.getLogger(SocketDemo.getClass.getName)
  Logger.getLogger("org").setLevel(Level.ERROR)

//  var checkpointLocation = "./structured_checkpoint"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions",4)
      .getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",9999)
      .load()

    val words = lines.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count()

    wordCounts.printSchema()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

      query.awaitTermination()
  }
}
