package com.yiban.spark.ml.dev

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by 10000347 on 2016/4/11.
  */
object YibanImplict01 {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  val conf = new SparkConf().setAppName("ALSExample")
  val sc = new SparkContext(conf)
  var data = sc.textFile("hdfs://yiban.isilon:8020/ml/implict_data.dat")
  // RDD  [user obj]
  var sorted = data map {_.split("::").take(2)} map { array => (array(0).toInt, array(1).toInt)} sortByKey()
  val hot_obj = sorted map {x => (x._2, 1)} reduceByKey { (x, y) => x + y }

  implicit val sortTuple = new Ordering[(Int, Int)] {
    override def compare(a: (Int, Int), b: (Int, Int)) = a._2.compare(b._2)
  }

  val hot_sort = hot_obj.top(30)(sortTuple)

  import scala.collection.mutable.{Set => MutSet}

  // [u [obj]
  val rate = sorted.groupByKey() mapValues {
    (x) =>  {
      val set = x.toSet
      val hot_rand = for (i <- 1 to set.size ) yield hot_sort((Math.random*30).toInt)._1
      val set_rand = hot_rand.toSet
      val out = MutSet[(Int, Int)]()
      for(v <- set) {
        out += ((v, 1))
      }
      for(v <- set_rand) {
        if ( !set.contains(v)) {
          out += ((v, 0))
        }
      }
      out.toArray
    }
  }

  //(1219538,Array((13198740,0), (3353572,1), (3351054,1), (3352416,0)))
//(1219538,(1938037,0)), (1219538,(13342980,0)), (1219538,(3353572,1)), (1219538,(3351054,1)),
  val finial = rate.flatMapValues {
    (value) => value
  }
}
