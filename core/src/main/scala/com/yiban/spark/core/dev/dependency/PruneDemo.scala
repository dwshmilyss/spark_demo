package com.yiban.spark.core.dev.dependency

import com.yiban.spark.core.dev.BaseSparkContext
import org.apache.spark.rdd.RDD

object PruneDemo extends BaseSparkContext {
  def main(args: Array[String]): Unit = {
    val sparkContext = getLocalSparkContext("RangeDemo")

    def mapPartitionWithIndex(rdd:RDD[Tuple2[String, Int]]):Array[(String, List[Tuple2[String, Int]])] = {
      val res = rdd.mapPartitionsWithIndex((index, iter) => {
        var partition_index_map = scala.collection.mutable.Map[String, List[Tuple2[String, Int]]]()
        val partition_index = "partition_" + index
        partition_index_map(partition_index) = List[Tuple2[String, Int]]()
        while (iter.hasNext) {
          partition_index_map(partition_index) :+= iter.next()
        }
        partition_index_map.iterator
      })
      res.collect()
    }

    //one to one
    val rdd1 = sparkContext.makeRDD(List(("a", 2), ("d", 1), ("b", 8), ("c", 5), ("b", 7), ("c", 6)), 2)
    val  rdd2 = rdd1.sortByKey()
    println(rdd2.collect().mkString("\n"))

//    val rdd1_collect = mapPartitionWithIndex(rdd1)
//    println("rdd1 makeRDD after mapPartitionsWithIndex\n" + rdd1_collect.mkString("\n"))
//
//    val rdd2 = rdd1.sortByKey()
//    val rdd2_collect = mapPartitionWithIndex(rdd2)
//    println("rdd2 sortByKey after mapPartitionsWithIndex\n" + rdd2_collect.mkString("\n"))
//
//    /**
//     * 和sortBy算子一样(一个算子内会生成两个RDD)，filterByRange也是一个MapPartitionsRDD，它依赖一个PartitionPruningRDD
//     *
//     * PruneDependency(partition个数相同，因为是窄依赖，子RDD的partition为父RDD的partition的子集，子集为满足条件的元素的子集)
//     * 例如 父RDD为：
//     * (partition_0,List((a,2), (b,8), (b,7)))
//     * (partition_1,List((c,5), (c,6), (d,1)))
//     * 执行算子之后的子RDD为：
//     * (partition_0,List((a,2), (b,8), (b,7)))
//     * (partition_1,List((c,5), (c,6)))
//     */
//    val rdd3 = rdd2.filterByRange("a", "c")
//
//    val rdd3_collect = mapPartitionWithIndex(rdd3)
//    println("rdd3 filterByRange after mapPartitionsWithIndex\n" + rdd3_collect.mkString("\n"))
//    println("rdd1 filterByRange\n" + rdd3.collect().mkString("\n"))

    Thread.sleep(3600 * 1000)
    sparkContext.stop()
  }
}
