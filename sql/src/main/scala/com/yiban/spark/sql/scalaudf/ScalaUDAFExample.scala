package com.yiban.spark.sql.scalaudf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{StructType, DataType}

object ScalaUDAFExample {
  private class SumProductAggregateFunction extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = ???

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = ???

    override def bufferSchema: StructType = ???

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

    override def initialize(buffer: MutableAggregationBuffer): Unit = ???

    override def deterministic: Boolean = ???

    override def evaluate(buffer: Row): Any = ???

    override def dataType: DataType = ???
  }
}
