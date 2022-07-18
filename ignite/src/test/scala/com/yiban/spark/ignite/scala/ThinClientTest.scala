package com.yiban.spark.ignite.scala

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.apache.ignite.Ignition
import org.apache.ignite.client.{ClientCache, ClientRetryAllPolicy, IgniteClient, ThinClientKubernetesAddressFinder}
import org.apache.ignite.configuration.{ClientConfiguration, IgniteConfiguration}
import org.apache.ignite.kubernetes.configuration.KubernetesConnectionConfiguration
import org.apache.ignite.spark.IgniteContext
import org.apache.ignite.spark.IgniteDataFrameSettings.{FORMAT_IGNITE, OPTION_CONFIG_FILE, OPTION_TABLE}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.springframework.util.StopWatch

class ThinClientTest {
  var spark: SparkSession = _

  @BeforeEach
  def init() = {
    spark = SparkSession.builder()
      .appName("ignite")
      .master("local")
      .config("spark.executor.instances", "2")
      .getOrCreate()
  }

  @Test
  def testConnectIgnite() = {
    val cfg = new ClientConfiguration
    val kcfg = new KubernetesConnectionConfiguration
    kcfg.setNamespace("devtest")
    kcfg.setServiceName("ignite")
    kcfg.setAccountToken(LeadswarpDemo.getClass.getClassLoader.getResource("token").getPath)
    cfg.setAddressesFinder(new ThinClientKubernetesAddressFinder(kcfg))
    cfg.setRetryPolicy(new ClientRetryAllPolicy)
    val igniteClient:IgniteClient = Ignition.startClient(cfg)
    val cache:ClientCache[String, String] = igniteClient.getOrCreateCache("test_cache")
    cache.put("dw","123")
    println(s"value = ${cache.get("dw")}")
  }

  @AfterEach
  def close() = {
    spark.stop()
  }
}
