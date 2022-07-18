package com.yiban.spark.core.dev

import com.alibaba.fastjson.{JSON, JSONObject}
import com.yiban.spark.core.http.AsyncHttpClient
import okhttp3.{Headers, Request}
import org.apache.http.HttpHeaders
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

import scala.collection.JavaConverters._

object OkhttpDemo {
  def main(args: Array[String]): Unit = {
    val schema = StructType(Seq(
      StructField("Count", IntegerType, true),
      StructField("Message", StringType, true),
      StructField("SearchCriteria", StringType, true),
      StructField("Results", ArrayType(
        StructType(Seq(StructField("Make_ID", IntegerType), StructField("Make_Name", StringType)))
      ))))

    val requestUDF = udf((url: String) => {
      try {
        val headers:Headers = new Headers.Builder().add("content-type", "application/json").build();
        val res = AsyncHttpClient.get(url,headers)
        if (res != null) {
           JSON.parse(res)
        }
      } catch {
        case exception: Exception => null
      }
    }, schema)

    val spark: SparkSession = SparkSession.builder()
      .appName("OkhttpDemo")
      .master("local[2]")
      .getOrCreate()

    val df = spark.createDataFrame(List(Row("https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json")).asJava, StructType(Seq(StructField("url", StringType, true))))
    val resDF = df.withColumn("res",requestUDF(col("url")))
    resDF.show()
  }
}
