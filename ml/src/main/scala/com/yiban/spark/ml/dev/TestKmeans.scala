package com.yiban.spark.ml.dev

import java.util.logging.{Level, Logger}


import java.util.logging.{Level, Logger}

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by 10000347 on 2015/7/25.
 * data:10.21.3.126 /root/data/kmeans/kmeans_train_data.txt
 */
object TestKmeans {
  def main(args: Array[String]) {
    //设置日志级别，减少控制台日志输出
    Logger.getLogger("org.apache.spark").setLevel(Level.WARNING)
    if (args.length < 1) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val data = sc.textFile(args(0))
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble)))
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData,numClusters,numIterations)

    println("------Predict the existing line in the analyzed data file: "+args(0))
    println("Vector 1.0 2.1 3.8 belongs to clustering "+ clusters.predict(Vectors.dense("1.0 2.1 3.8".split(' ').map(_.toDouble))))
    println("Vector 5.6 7.6 8.9 belongs to clustering "+ clusters.predict(Vectors.dense("5.6 7.6 8.9".split(' ').map(_.toDouble))))
    println("Vector 3.2 3.3 6.6 belongs to clustering "+ clusters.predict(Vectors.dense("3.2 3.3 6.6".split(' ').map(_.toDouble))))
    println("Vector 8.1 9.2 9.3 belongs to clustering "+ clusters.predict(Vectors.dense("8.1 9.2 9.3".split(' ').map(_.toDouble))))
    println("Vector 6.2 6.5 7.3 belongs to clustering "+ clusters.predict(Vectors.dense("6.2 6.5 7.3".split(' ').map(_.toDouble))))

    println("-------Predict the non-existent line in the analyzed data file: ----------------")
    println("Vector 1.1 2.2 3.9  belongs to clustering "+ clusters.predict(Vectors.dense("1.1 2.2 3.9".split(' ').map(_.toDouble))))
    println("Vector 5.5 7.5 8.8  belongs to clustering "+ clusters.predict(Vectors.dense("5.5 7.5 8.8".split(' ').map(_.toDouble))))

    println("-------Evaluate clustering by computing Within Set Sum of Squared Errors:-----")
    val wssse = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = "+ wssse)
    sc.stop()
  }
}
