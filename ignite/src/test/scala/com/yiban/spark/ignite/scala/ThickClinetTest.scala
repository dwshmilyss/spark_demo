package com.yiban.spark.ignite.scala

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.apache.ignite.client.ClientCache
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.kubernetes.configuration.KubernetesConnectionConfiguration
import org.apache.ignite.spark.{IgniteContext, IgniteRDD}
import org.apache.ignite.spark.IgniteDataFrameSettings.{FORMAT_IGNITE, OPTION_CONFIG_FILE, OPTION_TABLE}
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.kubernetes.TcpDiscoveryKubernetesIpFinder
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.springframework.util.StopWatch

import java.net.URL

class ThickClinetTest {
  var spark: SparkSession = _
  var sc:SparkContext = _
//  val cfgPath: String = "hdfs://hadoop-hdfs-namenode:9000/ignite/node.xml"
  val cfgPath: String = "hdfs://hadoop-hdfs-namenode:9000/ignite/node_cpd2_client.xml"
//  val cfgPath: String = "hdfs://hadoop-hdfs-namenode:9000/ignite/node_cpd2_standalone.xml"

  @BeforeEach
  def init(): Unit = {
    spark = SparkSession.builder()
      .appName("ignite")
      .master("local")
      .config("spark.executor.instances", "2")
      .getOrCreate()
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())
    sc = spark.sparkContext
  }

  @Test
  def testConnectIgnite(): Unit = {
    val igniteContext: IgniteContext = new IgniteContext(spark.sparkContext, () => {
      val configuration = new IgniteConfiguration()
      configuration.setClientMode(true)
      val kcfg = new KubernetesConnectionConfiguration
      kcfg.setNamespace("devtest")
      kcfg.setServiceName("ignite")
      kcfg.setAccountToken(LeadswarpDemo.getClass.getClassLoader.getResource("token").getPath)
      val tcpDiscoveryKubernetesIpFinder = new TcpDiscoveryKubernetesIpFinder(kcfg)
      configuration.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(tcpDiscoveryKubernetesIpFinder))
    })
//    val cache = igniteContext.fromCache[Integer, Integer]("partitioned")
//    println("Name: "+cache.cacheName+"\tCount: "+cache.count())
//
//    cache.savePairs(sc.parallelize(1 to 10000, 10).map( item => (item, item)))
//    println("Name: "+cache.cacheName+"\tCount: "+cache.count())

//    val result = cache.sql("select _val from Integer where val > ? and val < ?", 10, 100)
//    result.show(10)


    val cache_str_str: IgniteRDD[String, String] = igniteContext.fromCache[String, String]("templatesevendays")
    cache_str_str.savePairs(sc.parallelize(Seq(("dw", "123"), ("zs", "111"))))
    println("value = " + cache_str_str.filter(_._1.equals("zs")).collect().head._2)
  }

  /**
   * 该方法测试失败，因为涉及到持久化，所以集群外的节点无法访问集群
   */
  @Test
  def testIgniteSQL():Unit = {
    val df = spark.read.format(FORMAT_IGNITE).option(OPTION_TABLE, "hash_cache").option(OPTION_CONFIG_FILE, cfgPath).load()
    df.show(10,false)
  }

  @AfterEach
  def close() = {
    spark.stop()
  }
}
