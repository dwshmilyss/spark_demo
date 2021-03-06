package com.yiban.spark.structured.dev

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * One recommended <window duration>, <slide duration> pair is 10, 5
  */
object StructuredNetworkWordCountWindowed {
  val logger:Logger = Logger.getLogger(StructuredNetworkWordCountWindowed.getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val port = 9999

    val windowSize = 10
    val slideSize = 5
    if (slideSize > windowSize) {
      System.err.println("<slide duration> must be less than or equal to <window duration>")
    }
    val windowDuration = s"$windowSize seconds"
    val slideDuration = s"$slideSize seconds"

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCountWindowed")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions",4)
      .config("spark.default.parallelism",4)
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .option("includeTimestamp", true)
      .load()

    // Split the lines into words, retaining timestamps
    val words = lines.as[(String, Timestamp)].flatMap(line =>
      line._1.split(" ").map(word => (word, line._2))
    ).toDF("word", "timestamp")

    // Group the data by window and word and compute the count of each group
    val windowedCounts = words.groupBy(
      window($"timestamp", windowDuration, slideDuration), $"word"
    ).count().orderBy("window")

    // Start running the query that prints the windowed word counts to the console
    val query = windowedCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }
}
