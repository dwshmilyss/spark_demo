package com.yiban.spark.sql.dev

import java.util.Properties

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}

object TestOozie {

  val username: String = "root"
  val password: String = "wenha0"

  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("spark_oozie_demo").setMaster("local[*]")
    val conf = new SparkConf().setAppName("spark_oozie_demo")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

      /**
        * 这里如果使用一个查询语句作为表名的话，一定要用括号 后面再起个别名
        */
    val sql = "(select cn,product,catagory,catagory_id,isPV,insertTime from DoubleIndex order by cn desc limit 100) temp"

    val url = "jdbc:mysql://10.21.3.120:3306/yiban_BI?characterEncoding=utf-8"

    val properties=new Properties()
    properties.put("user", username)
    properties.put("password", password)

    val reader = sqlContext.read.format("jdbc")
    reader.option("url",url)//数据库路径
    reader.option("dbtable",sql)//数据表名，也可以是一个查询语句
    reader.option("driver","com.mysql.jdbc.Driver")
    reader.option("user",username)
    reader.option("password",password)
    val df = reader.load()
    val table = "DoubleIndex_copy"
    df.write.mode(SaveMode.Append).jdbc(url,table,properties)

    sc.stop()
  }
}
