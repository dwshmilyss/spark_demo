package com.yiban.spark.sql.dev


import com.yiban.spark.sql.javaudf.MyFlatMapFunction
import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.col

import scala.collection.mutable

object ReadDataDemo {
  //限制控制台的打印输出
  val logger: Logger = Logger.getLogger("org.apache.spark")
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("ReadDataDemo").master("local[*]").getOrCreate()

  case class Sale(id: Int, productName: String, typeName: String, saleCount: Int)
  case class Person(name: String, sex: String, age: Long, height: Long)

  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir", "D:\\soft\\hadoop\\hadoop-2.8.5")
//        testReadDataFromTxt
//    testReadDataFromJSON
    testFlatMap()
  }

  def testFlatMap():Unit = {
    import spark.implicits._
    val path = ReadDataDemo.getClass.getClassLoader.getResource("data/person.json").getPath
    val ds = spark.read.json(path).as[Person]
    ds.map(row => row.name).flatMap(MyFlatMapFunction,Encoders.STRING).collect()
  }



  /**
    * 从txt中读取
    */
  def testReadDataFromTxt() = {
    import spark.implicits._
    //这里如果找不到 手动copy到 sql/target/data目录下
    val path = ReadDataDemo.getClass.getClassLoader.getResource("data/product.txt").getPath
    //    val saleDF = spark.read.textFile(path).map(_.split(",")).map(sale => Sale(sale(0).trim.toInt, sale(1), sale(2), sale(3).trim.toInt)).toDF()
    val tempDF = spark.sparkContext.textFile(path).map(_.split(",")).map(row => Row(row(0).trim.toInt, row(1), row(2), row(3).trim.toInt))
    //set schema structure
    import org.apache.spark.sql.types._
    val schema = StructType(
      Seq(
        StructField("id",IntegerType,true),
        StructField("productname",StringType,true),
        StructField("typename",StringType,true),
        StructField("salecount",IntegerType,true)
      )
    )
    val saleDF = spark.createDataFrame(tempDF,schema)
    saleDF.createOrReplaceTempView("product")
    val resDF = spark.sql("select * from (\nselect id,productname,typename,salecount,rank()over(partition by typename order by salecount desc) mm from product\n) where mm <= 3")
    resDF.show()
    //    println("========= extended=true ============")
    //    resDF.explain(true)
    //    println("========= extended=false ============")
    //    resDF.explain(false)
    spark.stop()
  }

  /**
    * 从json中读取  注意如果想直接使用json数据 必须每一行是一个完整的json串
    */
  def testReadDataFromJSON() = {
    import spark.implicits._
    import org.apache.spark.sql.types._
    //这里如果找不到 手动copy到 sql/target/data目录下
    val path = ReadDataDemo.getClass.getClassLoader.getResource("data/product.json").getPath

    val df = spark.read.json(path)
    df.printSchema()
    val saleDF = df.map(sale => Sale(sale.getAs[String]("id").toInt, sale.getAs[String]("PRODUCTNAME"), sale.getAs[String]("TYPENAME"), sale.getAs[String]("SALECOUNT").toInt)).toDF()
    saleDF.printSchema()
    saleDF.createOrReplaceTempView("product")
    val resDF = spark.sql("select * from (\nselect id,productname,typename,salecount,rank()over(partition by typename order by salecount desc) mm from product\n) where mm <= 3")
    resDF.show()

    //    try {
    //      val schema = StructType(Seq(
    //        StructField("id", IntegerType),
    //        StructField("productName", StringType),
    //        StructField("typeName", StringType),
    //        StructField("saleCount", IntegerType)
    //      ))
    //
    //      val encoder = RowEncoder(schema)
    //
    //      spark.read.json(path).flatMap(new FlatMapFunction[Row, Row] {
    //        override def call(r: Row): java.util.Iterator[Row] = {
    //          val list = new java.util.ArrayList[Row]()
    //          val datas = r.getAs[mutable.WrappedArray.ofRef[Row]]("RECORDS")
    //          datas.foreach(data => {
    //            list.add(Row(r.getAs[Int]("id"), data.getAs[String]("PRODUCTNAME"), data.getAs[String]("TYPENAME"), data.getAs[Int]("SALECOUNT")))
    //          })
    //          list.iterator()
    //        }
    //      }, encoder).show()
    //    }catch {
    //      case e:Exception => e.printStackTrace()
    //    }

    //解析嵌套的json 格式为{id:xxx,data:[{"name"=xx,age=xx},{name=xx,age=xx}]}
    //    try {
    //      val schema = StructType(Seq(
    //        StructField("id", IntegerType),
    //        StructField("productName", StringType),
    //        StructField("typeName", StringType),
    //        StructField("saleCount", IntegerType)
    //      ))
    //
    //      val encoder = RowEncoder(schema)
    //
    //      val df = spark.read.json(path)
    //        .flatMap(new FlatMapFunction[Row, Row] {
    //          override def call(r: Row): java.util.Iterator[Row] = {
    //            val list = new java.util.ArrayList[Row]()
    //            val datas = r.getAs[mutable.WrappedArray.ofRef[Row]]("data")
    //            datas.foreach(data => {
    //              list.add(Row(r.getAs[Int]("id"), data.getAs[Int](1), data.getAs[String](0)))
    //            })
    //            list.iterator()
    //          }
    //        }, encoder)
    //      df.show()
    //    } catch {
    //      case e:Exception => println(e.getMessage)
    //    }
    spark.close()
  }
}
