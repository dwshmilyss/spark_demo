package com.yiban.spark.hbase

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

object SparkSQLOnHbase {
  def main(args: Array[String]): Unit = {


    val starttime=System.nanoTime


    // 本地模式运行,便于测试
    val sparkConf = new SparkConf().setMaster("local").setAppName("HBaseTest")

    // 创建hbase configuration
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.property.clientPort", "2181");
    hBaseConf.set("hbase.zookeeper.quorum", "192.168.10.82");
    //var con = ConnectionFactory.createConnection(hBaseConf)

    //var table = con.getTable(TableName.valueOf(""))

    hBaseConf.set(TableInputFormat.INPUT_TABLE,"test_lcc_mycase")

    // 创建 spark context
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // 从数据源获取数据
    var hbaseRDD = sc.newAPIHadoopRDD(hBaseConf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])

    // 将数据映射为表  也就是将 RDD转化为 dataframe schema
    val mycase = hbaseRDD.map(r=>(
      Bytes.toString(r._2.getValue(Bytes.toBytes("case_lizu"),Bytes.toBytes("c_code"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("case_lizu"),Bytes.toBytes("c_rcode"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("case_lizu"),Bytes.toBytes("c_region"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("case_lizu"),Bytes.toBytes("c_cate"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("case_lizu"),Bytes.toBytes("c_start"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("case_lizu"),Bytes.toBytes("c_end"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("case_lizu"),Bytes.toBytes("c_start_m"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("case_lizu"),Bytes.toBytes("c_end_m"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("case_lizu"),Bytes.toBytes("c_name"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("case_lizu"),Bytes.toBytes("c_mark")))

    )).toDF("c_code","c_rcode","c_region","c_cate","c_start","c_end","c_start_m","c_end_m","c_name","c_mark")

    mycase.registerTempTable("mycase")


    // 测试
    val df5 = sqlContext.sql("select * from  mycase  ")
    df5.show()
    println(df5.count())
    // df5.collect().foreach(print(_))




    val endtime=System.nanoTime
    val delta=endtime-starttime
    println(delta/1000000d)

  }
}
