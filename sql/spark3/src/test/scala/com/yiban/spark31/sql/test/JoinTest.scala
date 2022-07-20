package com.yiban.spark31.sql.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.{BeforeEach, Test}

class JoinTest {
  @BeforeEach
  def init(): Unit = {
    val logger: Logger = Logger.getLogger("org.apache.spark")
    Logger.getLogger("org").setLevel(Level.ERROR)
  }

  @Test
  def testPushDownWindow():Unit = {
    val sparkSession = SparkSession.builder()
      .appName("testPushDownProjection")
      .master("local[2]")
      .getOrCreate()
    import sparkSession.implicits._
    val empSalay1 = Seq(
      Salary("sales", 1, 5000),
      Salary("personnel", 2, 3000),
      Salary("sales", 3, 4800),
      Salary("sales", 4, 4800),
      Salary("personnel", 5, 3500),
    ).toDS()

    val empSalay2 = Seq(
      Salary("sales", 1, 5000),
      Salary("personnel", 2, 3000),
      Salary("sales", 3, 4800),
      Salary("sales", 6, 1000),
      Salary("personnel", 7, 2000),
    ).toDS()

    println("leftsemi")
    empSalay1.join(empSalay2,empSalay1("id") === empSalay2("id"),"leftsemi").show(false)
    println("leftanti")
    empSalay1.join(empSalay2,empSalay1("id") === empSalay2("id"),"leftanti").show(false)
    println("leftouter")
    empSalay1.join(empSalay2,empSalay1("id") === empSalay2("id"),"leftouter").show(false)
    println("rightouter")
    empSalay1.join(empSalay2,empSalay1("id") === empSalay2("id"),"rightouter").show(false)
    println("cross")
    empSalay1.join(empSalay2,empSalay1("id") === empSalay2("id"),"cross").show(false)
    empSalay1.crossJoin(empSalay2).show(false)
    println("inner")
    empSalay1.join(empSalay2,empSalay1("id") === empSalay2("id"),"inner").show(false)
    println("full")
    empSalay1.join(empSalay2,empSalay1("id") === empSalay2("id"),"full").show(false)
    println("outer")
    empSalay1.join(empSalay2,empSalay1("id") === empSalay2("id"),"outer").show(false)
    println("fullouter")
    empSalay1.join(empSalay2,empSalay1("id") === empSalay2("id"),"fullouter").show(false)
  }
}
