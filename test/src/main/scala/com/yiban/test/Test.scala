package com.yiban.test

object Test {
  def main(args: Array[String]): Unit = {
    List(1 to 10 : _*).foreach(println)
    val mylist =List(1,2,3,4,5)
    sum(mylist:_*)
  }


  def sum(args: Int*) = {
    var result = 0
    for (arg <- args) result += arg
    result
  }
}
