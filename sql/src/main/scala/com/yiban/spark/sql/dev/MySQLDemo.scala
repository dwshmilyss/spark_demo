package com.yiban.spark.sql.dev

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * 四种访问mysql的方式(JDBC)
  */
object MySQLDemo {

  val url = "jdbc:mysql://10.21.3.120:3306/test?user=root&password=wenha0"
  val url1 = "jdbc:mysql://localhost:3306/test?user=root&password=120653"

  val spark = SparkSession
    .builder()
    .appName("MySQLDemo")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]) {
    test6("")
  }


  /**
    * 加载整个表
    */
  def test1(): Unit = {

    /**
      * 可以通过load加载数据
      */
    //    val reader = spark.read.format("jdbc")
//    reader.option("url", url)
//    reader.option("dbtable", "test")
//    reader.option("driver", "com.mysql.jdbc.Driver")
//    reader.option("user", "root")
//    reader.option("password", "wenha0")
//    val df = reader.load()

    /**
      * 可以直接读取数据
      */
    val prop = new Properties();
    val df = spark.read.jdbc(url,"test",prop)
    //数据的条数
    println(df.count())
    //dataFrame的分区数 默认是1 如果表中数据量太大 容易OOM
    println(df.rdd.partitions.size)
  }

  /**
    * 加载部分数据 根据字段分区 只能是数字型的字段
    * 这种方式只是确定分区规则  并不过滤数据
    */
  def test2(): Unit ={
    val lowerBound = 1
    val upperBound = 5
    val numPartitions = 2

    val prop = new Properties()
    val df = spark.read.jdbc(url, "test", "id", lowerBound, upperBound, numPartitions, prop)
    println(df.count())
    //dataFrame的分区数 默认是1 如果表中数据量太大 容易OOM
    println(df.rdd.partitions.size)
  }

  /**
    * 根据任意字段分区
    */
  def test3(): Unit ={
    val predicates = Array[String]("userId <= 50",
      "platform = 'mobile'")

    val prop = new Properties()
    val df = spark.read.jdbc(url, "test",  predicates, prop)
    println(df.count())
    //dataFrame的分区数 默认是1 如果表中数据量太大 容易OOM
    println(df.rdd.partitions.size)
  }

  /**
    * 直接写SQL
    */
  def test4(): Unit ={
    val prop = new Properties();
    val df = spark.read.jdbc(url,"test",prop)
    df.createOrReplaceTempView("test")
    val data = spark.sqlContext.sql("select * from test where User_Id <= 50")
    data.show()
    // 将表进行缓存，并查询两次，通过 Web Console 监控执行的时间
    spark.sqlContext.cacheTable("test")
    // 对比两次执行的时间
    data.show()
    //清空缓存
    spark.sqlContext.clearCache()
    println(df.rdd.partitions.size)
  }

  def test5(): Unit ={
    val df = spark.read.load("sql/src/main/resources/users.parquet")
    df.select("name", "favorite_color").write.save("sql/src/main/resources/namesAndFavColors.parquet")
  }

  /**
    * 测试 分组topN
    * @param param
    */
  def test6(param:String)={
    val prop = new Properties();
    val df = spark.read.jdbc(url1,"product",prop)
    df.createOrReplaceTempView("product")
//    val data = spark.sqlContext.sql("select * from product")
//    val data = spark.sqlContext.sql("SELECT TBL.ID,TBL.PRODUCTNAME,TBL.TYPENAME,TBL.SALECOUNT\nFROM product TBL\nLEFT JOIN product L_TBL ON TBL.TYPENAME = L_TBL.TYPENAME AND TBL.SALECOUNT < L_TBL.SALECOUNT\nGROUP BY TBL.ID,TBL.PRODUCTNAME,TBL.TYPENAME,TBL.SALECOUNT\nHAVING COUNT(L_TBL.ID) < 3\nORDER BY TBL.TYPENAME,TBL.SALECOUNT DESC")
    val data = spark.sqlContext.sql("select * from (\nselect id,productname,typename,salecount,rank()over(partition by typename order by salecount desc) mm from product\n) where mm <= 3")
    data.show()
  }

}
