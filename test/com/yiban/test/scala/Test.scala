package com.yiban.test.scala

import org.junit
import org.junit.{Assert, Test}

@Test
class Test extends Assert{
 @junit.Test
 def test1(): Unit ={
  val str = "2018-02-22T00:00:00+08:00|~|200|~|/test?pcid=DEIBAH&siteid=3"
  val arr:Array[String] = str.split("\\|~\\|",-1)
  println(arr(2).split("[=|&]", -1).length)
  println(arr(2).split("[=|&]", -1)(1))
  println(arr(2).split("[=|&]", -1)(3))
 }
}
