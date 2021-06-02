package org.apache.spark.scheduler

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 测试自定义的SparkListener
 */
object TestSparkListener {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\Program Files\\hadoop-2.8.5")
    val sparkConf = new SparkConf().setAppName("TestSparkListener").setMaster("local")
    sparkConf.set("spark.metrics.conf.driver.source.jvm.class","org.apache.spark.metrics.source.JvmSource")
    sparkConf.set("spark.metrics.conf.executor.source.jvm.class","org.apache.spark.metrics.source.JvmSource")
    sparkConf.set("spark.metrics.conf.master.source.jvm.class","org.apache.spark.metrics.source.JvmSource")
    sparkConf.set("spark.metrics.conf.worker.source.jvm.class","org.apache.spark.metrics.source.JvmSource")
    val sc = new SparkContext(sparkConf)
    /*  sc.setJobGroup("test1","testdesc")
      val completedJobs= sc.jobProgressListener*/
    sc.addSparkListener(new MySparkListener)
    val rdd1 = sc.parallelize(List(('a', 'c', 1), ('b', 'a', 1), ('b', 'd', 8)))
    val rdd2 = sc.parallelize(List(('a', 'c', 2), ('b', 'c', 5), ('b', 'd', 6)))
    val rdd3 = rdd1.union(rdd2).map {
      x => {
        //todo 这个异常是为了测试MySparkListener用的
        //        throw new Exception("这是一个异常")
        Thread.sleep(500)
        x
      }
    }.count()
    println(rdd3)
    Thread.sleep(3600 * 1000)
    sc.stop()
  }
}
