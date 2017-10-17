package org.apache.spark.scheduler

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 测试自定义的SparkListener
  */
object TestSparkListener {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("TestSparkListener").setMaster("local")
    val sc = new SparkContext(sparkConf)
    /*  sc.setJobGroup("test1","testdesc")
      val completedJobs= sc.jobProgressListener*/
    sc.addSparkListener(new MySparkListener)
    val rdd1 = sc.parallelize(List(('a', 'c', 1), ('b', 'a', 1), ('b', 'd', 8)))
    val rdd2 = sc.parallelize(List(('a', 'c', 2), ('b', 'c', 5), ('b', 'd', 6)))
    val rdd3 = rdd1.union(rdd2).map {
      x => {
        Thread.sleep(500)
        x
      }
    }.count()
    println(rdd3)
    sc.stop()
  }
}
