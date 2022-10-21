package com.yiban.spark.sql.test

import com.yiban.spark.sql.dev.ReadDataDemo
import org.junit.jupiter.api.Test

class Demo {
  @Test
  def test():Unit = {
    val path = ReadDataDemo.getClass.getClassLoader.getResource("product.txt").getPath
    println(s"path = ${path}")
  }


}
