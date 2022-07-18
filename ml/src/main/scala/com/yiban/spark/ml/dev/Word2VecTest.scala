package com.yiban.spark.ml.dev

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by 10000347 on 2016/5/10.
  */
object Word2VecTest {

  val conf = new SparkConf().setAppName("Word2VecTest")
  val sc = new SparkContext(conf)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  //向量维度（词性特征）
  val VLEN = 400

  val data = sc.textFile("hdfs://yiban.isilon:8020/ml/wiki.zh.text.vector") map {
    _.split(" ")
  } filter {
    _.length == VLEN + 1
  } map { array => {
    val v = for (i <- 1 to VLEN) yield array(i).toFloat
    (array(0), v)
  }
  }

  data.persist()
//  data.saveAsTextFile("hdfs://yiban.isilon:8020/ml/word2vec.txt")


  /**
    * 余弦相似度
    *
    * @param data
    * @param as
    * @return
    */
  def similarityTopNByCOS(data: RDD[(String, scala.collection.immutable.IndexedSeq[Float])], as: String): Array[(Double, String)] = {
    val va = data.lookup(as)(0)
    var norma = 0.0
    for (i <- 0 until VLEN) {
      norma += va(i) * va(i)
    }
    norma = Math.sqrt(norma)

    val result = data map { case (k, vb) => {
      var sum = 0.0
      var normb = 0.0
      for (i <- 0 until VLEN) {
        sum += va(i) * vb(i)
        normb += vb(i) * vb(i)
      }
      (sum / norma / Math.sqrt(normb), k)
    }
    } top (20)

    result
  }

  /**
    * 欧几里得距离计算相似度
    * @param data
    * @param as
    * @return
    */
  def similarityTopNByEuclid(data: RDD[(String, scala.collection.immutable.IndexedSeq[Float])], as: String): Array[(Double, String)] = {
    val va = data.lookup(as)(0)

    val result = data map { case (k, vb) => {
      var sum = 1.0
      for (i <- 0 until VLEN) {
        sum += ((va(i) - vb(i)) * (va(i) - vb(i)))
      }

      (1 /  Math.sqrt(sum) , k)
    }
    } top (20)

    result
  }

  def query(data: RDD[(String, scala.collection.immutable.IndexedSeq[Float])], as: String) = {
//    val res = similarityTopNByCOS(data, as)
    val res = similarityTopNByEuclid(data, as)

    println("相似度\t 匹配词 ")
    for (i <- res) println(i._1 + "\t" + i._2)

  }

  query(data,"和尚")

}
