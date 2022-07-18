package com.yiban.spark.core.dev

import okhttp3.{Headers, OkHttpClient, Request, Response}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions.{col, udf, from_json, explode}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

object OkhttpDemo1 {

  case class RestAPIRequest (url: String)

  def main(args: Array[String]): Unit = {
    val restApiSchema = StructType(List(
      StructField("Count", IntegerType, true),
      StructField("Message", StringType, true),
      StructField("SearchCriteria", StringType, true),
      StructField("Results", ArrayType(
        StructType(List(
          StructField("Make_ID", IntegerType, true),
          StructField("Make_Name", StringType, true)
        ))
      ), true)
    ))

    val executeRestApiUDF = udf(new UDF1[String, String] {
      override def call(url: String) = {
        ExecuteHttpGet(url).getOrElse("")
      }
    }, StringType)


    val spark: SparkSession = SparkSession.builder()
      .appName("OkhttpDemo")
      .master("local[2]")
      .getOrCreate()

    val restApiCallsToMake = spark.createDataFrame(Seq(RestAPIRequest("http://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json")))
    val source_df = restApiCallsToMake.toDF()

    val execute_df = source_df
      .withColumn("result", executeRestApiUDF(col("url")))
      .withColumn("result", from_json(col("result"), restApiSchema))

    execute_df.select(explode(col("result.Results")).alias("makes"))
      .select(col("makes.Make_ID"), col("makes.Make_Name"))
      .show(false)
  }

  def ExecuteHttpGet(url: String) : Option[String] = {

    val client: OkHttpClient = new OkHttpClient();

    val headerBuilder = new Headers.Builder
    val headers = headerBuilder
      .add("content-type", "application/json")
      .build

    val result = try {
      val request = new Request.Builder()
        .url(url)
        .headers(headers)
        .build();

      val response: Response = client.newCall(request).execute()
      response.body().string()
    }
    catch {
      case _: Throwable => null
    }
    Option[String](result)
  }
}
