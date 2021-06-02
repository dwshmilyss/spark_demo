package com.yiban.spark.core.dev.dependency.narrow

import com.yiban.spark.core.dev.BaseSparkContext
import org.apache.spark.rdd.RDD

object OneToOneDemo extends BaseSparkContext {
  def main(args: Array[String]): Unit = {
    val sparkContext = getLocalSparkContext("OneToOneDemo")
    //one to one
    val rdd1 = sparkContext.textFile("file:///d:/a.txt")
    //one to one
    val rdd2 = rdd1.flatMap((_.split((" "))))
    //one to one
    val rdd3 = rdd2.map((_, 1))
    //shuffle
    val rdd4 = rdd3.reduceByKey(_ + _)
    //one to one
    val rdd5: RDD[Tuple2[String, Int]] = rdd4.map(x => x)
    //shuffle 这里rdd6是个MapPartitionRDD(rdd6) 但是里面的依赖是->shuffledRDD(rdd6)->MapPartitionRDD(rdd6)->MapPartitionRDD(rdd5)
    //    val rdd6 = rdd5.sortBy(kv => kv._2, false)
    val rdd6 = rdd5.sortByKey()

    //    logInfo(rdd1.collect().mkString("{" , ",", "}"))

    //    val list = rdd2.mapPartitionsWithIndex((index, iterator) => {
    //      var partitionMap = scala.collection.mutable.Map[String, List[String]]()
    //      var partitionName: String = "partition_" + index
    //      partitionMap(partitionName) = List[String]()
    //      while (iterator.hasNext) {
    //        partitionMap(partitionName) :+= iterator.next()
    //      }
    //      partitionMap.iterator
    //    }).collect()
    //
    //
    //    val tupleList = rdd4.mapPartitionsWithIndex((index, iterator) => {
    //      var partitionMap = scala.collection.mutable.Map[String, List[Tuple2[String, Int]]]()
    //      var partitionName: String = "partition_" + index
    //      partitionMap(partitionName) = List[Tuple2[String, Int]]()
    //      while (iterator.hasNext) {
    //        partitionMap(partitionName) :+= iterator.next()
    //      }
    //      partitionMap.iterator
    //    }).collect()

        println(rdd4.collect().mkString("{", ",", "}"))
        println(rdd6.collect().mkString("{", ",", "}"))
    //    println(tupleList.mkString("{", ",", "}"))
    Thread.sleep(3600 * 1000)
    sparkContext.stop()
  }
}
