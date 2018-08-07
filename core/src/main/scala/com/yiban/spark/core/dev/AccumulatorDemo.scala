package com.yiban.spark.core.dev

import org.apache.spark.{AccumulableParam, SparkConf, SparkContext}
import scala.collection
import scala.collection.mutable
import scala.util.Random
import org.apache.spark.util.AccumulatorV2
/**
  * Spark累加器特性：
  *  数据只能在executor上进行写操作，在driver上进行读操作
  */
object AccumulatorDemo {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*,4]").setAppName("accumulator")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array(
      "hadoop,spark,hbase",
      "spark,hbase,hadoop",
      "",
      "spark,hive,hue",
      "spark,hadoop",
      "spark,,hadoop,hive",
      "spark,hbase,hive",
      "hadoop,hbase,hive",
      "hive,hbase,spark,hadoop",
      "hive,hbase,hadoop,hue"
    ), 2)
    // 需求一：实现WordCount程序，同时统计输入的记录数量以及最终输出结果的数量
    val inputRecords = sc.accumulator(0, "Input Record Size")
    val outputRecords = sc.accumulator(0, "Output Record Size")
    rdd.flatMap(line => {
      // 累计数量
      inputRecords += 1
      val nline = if (line == null) "" else line
      // 进行数据分割、过滤、数据转换
      nline.split(",")
        .map(word => (word.trim, 1)) // 数据转换
        .filter(_._1.nonEmpty) // word非空，进行数据过滤
    })
      .reduceByKey(_ + _)
      .foreachPartition(iter => {
        iter.foreach(record => {
          // 累计数据
          outputRecords += 1
          println(record)
        })
      })

    println(s"Input Size:${inputRecords.value}")
    println(s"Ouput Size:${outputRecords.value}")


    // 需求二：假设wordcount的最终结果可以在driver/executor节点的内存中保存下，要求不通过reduceByKey相关API实现wordcount程序
    /**
      * 1. 每个分区进行wordcount的统计，将结果保存到累加器中
      * 2. 当分区全部执行完后，各个分区的累加器数据进行聚合操作
      * 3. 在spark2.0以后已过期
      */
    val mapAccumulable = sc.accumulable(mutable.Map[String, Int]())(MapAccumulableParam)//MapAccumulableParam是强制转换
    try
      rdd.foreachPartition(iter => {
        val index = Random.nextInt(2) // index的取值范围[0,1]
        iter.foreach(line => {
          val r = 1 / index
          print(r)
          val nline = if (line == null) "" else line
          // 进行数据分割、过滤、数据转换
          nline.split(",")
            .filter(_.trim.nonEmpty) // 过滤空单词
            .map(word => {
            mapAccumulable += word // 统计word出现的次数
          })
        })
      })
    catch {
      case e: Exception => println(s"异常:${e.getMessage}")
    }
    println("result================")
    mapAccumulable.value.foreach(println)


    /**
      * spark2.0以后内置了Long和Double类型的累加器
      * sc.longAccumulator();
      * sc.doubleAccumulator();
      * add方法：赋值操作
         value方法：获取累加器中的值
         merge方法：该方法特别重要，一定要写对，这个方法是各个task的累加器进行合并的方法（下面介绍执行流程中将要用到）
         iszero方法：判断是否为初始值
         reset方法：重置累加器中的值
         copy方法：拷贝累加器
      */
    val accum = sc.longAccumulator("longAccum") //统计奇数的个数
    val sum = sc.parallelize(Array(1,2,3,4,5,6,7,8,9),2).filter(n=>{
        if(n%2!=0) accum.add(1L)
        n%2==0
      }).reduce(_+_)

    println("sum: "+sum)
    println("accum: "+accum.value)

    /**
      * 注意：
      * 1、累加器不会改变spark的lazy的计算模型，所以像map这样的transformation还没有真正的执行，从而累加器的值也就不会更新。
      */
    val numberRDD = sc.parallelize(Array(1,2,3,4,5,6,7,8,9),2).map(n=>{
      accum.add(1L)
      n+1
    })
    println("accum: "+accum.value)


    /**
      * 注意：
      * 2、如果触发了2次transformation操作，那么累加器就会执行2次，如果要结果一致，可以对transformation的rcd进行cache。这样就不会计算2次了
      */
    numberRDD.count//要想2次结果一致，这里要改为 numberRDD.cache().count
    println("accum1:"+accum.value)
    numberRDD.reduce(_+_)
    println("accum2: "+accum.value)


    /**
      * 测试Spark2.0自定义累加器
      * 利用自定义的累加器收集过滤操作中被过滤掉的元素
      */
    val logAccum = new LogAccumulator
    sc.register(logAccum, "logAccum")
    val sum1 = sc.parallelize(Array("1", "2a", "3", "4b", "5", "6", "7cd", "8", "9"), 2).filter(line => {
      val pattern = """^-?(\d+)"""
      val flag = line.matches(pattern)
      if (!flag) {
        logAccum.add(line)
      }
      flag
    }).map(_.toInt).reduce(_ + _)

    println("sum1: " + sum1)
    for (v <- logAccum.value.toArray()) print(v + " ")
    println()
    sc.stop()
  }
}


object MapAccumulableParam extends AccumulableParam[mutable.Map[String, Int], String] {
  /**
    * 添加一个string的元素到累加器中
    *
    * @param r
    * @param t
    * @return
    */
  override def addAccumulator(r: mutable.Map[String, Int], t: String): mutable.Map[String, Int] = {
    r += t -> (1 + r.getOrElse(t, 0))
  }

  /**
    * 合并两个数据
    *
    * @param r1
    * @param r2
    * @return
    */
  override def addInPlace(r1: mutable.Map[String, Int], r2: mutable.Map[String, Int]): mutable.Map[String, Int] = {
    r2.foldLeft(r1)((a, b) => {
      a += b._1 -> (a.getOrElse(b._1, 0) + b._2)
    })
  }

  /**
    * 返回初始值
    *
    * @param initialValue
    * @return
    */
  override def zero(initialValue: mutable.Map[String, Int]): mutable.Map[String, Int] = initialValue
}

/**
  * spark2.0 自定义累加器
  */
import org.apache.spark.util.AccumulatorV2
class LogAccumulator extends AccumulatorV2[String, java.util.Set[String]]{
  private val _logArray: java.util.Set[String] = new java.util.HashSet[String]()

  override def isZero: Boolean = {
    _logArray.isEmpty
  }

  override def reset(): Unit = {
    _logArray.clear()
  }

  override def add(v: String): Unit = {
    _logArray.add(v)
  }

  override def merge(other: AccumulatorV2[String, java.util.Set[String]]): Unit = {
//    other match {
//      case o: LogAccumulator => _logArray.addAll(o.value)
//    }
    _logArray.addAll(other.value)
  }

  override def value: java.util.Set[String] = {
//    java.util.Collections.unmodifiableSet(_logArray)
    return _logArray
  }

  override def copy(): AccumulatorV2[String, java.util.Set[String]] = {
    val newAcc = new LogAccumulator()
    newAcc.synchronized{
      newAcc._logArray.addAll(_logArray)
    }
    newAcc
  }
}
