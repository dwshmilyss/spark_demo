package com.yiban.spark.sql.scalaudf

import com.yiban.spark.sql.scalaudf.ScalaUDFDemo.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

object ScalaUDAFExample {

  private class SumProductAggregateFunction extends UserDefinedAggregateFunction {
    // Input  = (Double price, Long quantity)
    override def inputSchema: StructType = new StructType().add("price", DoubleType).add("quantity", LongType)


    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val sum = buffer.getDouble(0)
      // Intermediate result to be updated
      val price = input.getDouble(0)
      // First input parameter
      val qty = input.getLong(1) // Second input parameter
      buffer.update(0, sum + (price * qty))
    }

    // Output = (Double total)
    override def bufferSchema: StructType = new StructType().add("total", DoubleType)

    // Merge intermediate result sums by adding them
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0))
    }

    // Initialize the result to 0.0
    override def initialize(buffer: MutableAggregationBuffer): Unit = buffer.update(0, 0.0)

    //true: our UDAF's output given an input is deterministic
    override def deterministic: Boolean = true

    // THe final result will be contained in 'buffer'
    override def evaluate(buffer: Row): Any = buffer.getDouble(0)

    override def dataType: DataType = DoubleType
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Scala UDAF Example")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    def sqlContext = spark.sqlContext

    val path = """file:///D:/source_code/sparkDemo/sql/src/main/resources/data/inventory.json"""
    val testDF = sqlContext.read.json(path)

    testDF.createOrReplaceTempView("inventory")

    sqlContext.udf.register("SUMPRODUCT", new SumProductAggregateFunction)
    sqlContext.sql("SELECT Make, SUMPRODUCT(RetailValue,Stock) AS InventoryValuePerMake FROM inventory GROUP BY Make").show()
  }
}
