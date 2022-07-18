package com.yiban.spark.ignite.scala

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.apache.ignite.Ignition
import org.apache.ignite.client.{ClientCache, ClientRetryAllPolicy, IgniteClient, ThinClientKubernetesAddressFinder}
import org.apache.ignite.spark.IgniteDataFrameSettings
import org.apache.ignite.spark.IgniteDataFrameSettings.{FORMAT_IGNITE, OPTION_CONFIG_FILE, OPTION_SCHEMA, OPTION_TABLE}
import org.apache.spark.sql.SparkSession
import org.springframework.util.StopWatch
import org.apache.ignite.configuration.{ClientConfiguration, IgniteConfiguration}
import org.apache.ignite.kubernetes.configuration.KubernetesConnectionConfiguration
import org.apache.ignite.spark.{IgniteContext, IgniteRDD}
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.kubernetes.TcpDiscoveryKubernetesIpFinder

import java.net.URL

object LeadswarpDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())
//    val cfgPath: String = "hdfs://hdp1-pre1.leadswarp.com:8020/ignite_run_file/node.xml"
    val cfgPath: String = "hdfs://hadoop-hdfs-namenode:9000/ignite/node.xml"
    println(cfgPath)

    val df = spark.read
      .format(FORMAT_IGNITE) // Data source type.
      .option(OPTION_TABLE, "IDENTITY_T90") // Table to read.
      .option(OPTION_SCHEMA, "PUBLIC") // Table to read.
      .option(OPTION_CONFIG_FILE, cfgPath) // Ignite config.
      .load()

    df.createOrReplaceTempView("identity_test")
    val stopWatch: StopWatch = new StopWatch()
    stopWatch.start()
    //    val igniteDF = spark.sql("select * from identity_test  where id = '1'")
    val igniteDF = spark.sql("select * from identity_test order by contact_id desc limit 1")
    igniteDF.explain(true)
    println(igniteDF.queryExecution.executedPlan)
    igniteDF.show()
    stopWatch.stop()
    println(stopWatch.getTotalTimeMillis) //25105 ,17668

    spark.stop()
  }
}
