package com.yiban.spark.core.dev

import com.yiban.spark.core.http.AsyncHttpClient
import okhttp3.{Headers, OkHttpClient, Request, RequestBody, Response}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions.{col, explode, from_json, udf}
import org.apache.spark.sql.types._

import javax.xml.bind.DatatypeConverter
import scala.collection.JavaConverters._

object OkhttpDemo2 {

  case class RestAPIRequest (url: String)

  def main(args: Array[String]): Unit = {
    ExecuteHttpGet("https://linkflow.wisers.com.cn/sentiment-dl-common-gpu-lf/sentiment/document").getOrElse()
  }

  def ExecuteHttpGet(url: String) : Option[String] = {

    val client: OkHttpClient = new OkHttpClient();

    val headerBuilder = new Headers.Builder
    val encoding = DatatypeConverter.printBase64Binary("linkflow:Q2bpSsnqS=HA6q*m".getBytes("UTF-8"))
    val headers = headerBuilder
      .add("content-type", "application/json")
      .add("Authorization", "Basic " + encoding)
      .build

//    val body = AsyncHttpClient.buildFormBody(Map("docid" -> "12345","text" -> "这本书很赞！").asJava)
    val body = RequestBody.create("{\"docid\":\"12345\",\"text\":\"这本书很好！！！\"}",okhttp3.MediaType.parse("application/json;charset=UTF-8"))
    val result = try {
      val request = new Request.Builder()
        .url(url)
        .headers(headers)
        .post(body)
        .build();

      val response: Response = client.newCall(request).execute()
      val res = response.body().string()
      println(res)
      res
    }
    catch {
      case _: Throwable => null
    }
    Option[String](result)
  }
}
