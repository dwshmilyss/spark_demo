package org.apache.spark.scheduler

import org.apache.spark.scheduler._

class MySparkListener extends SparkListener{

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    println("*************************************************")
    println("app:end")
    println("*************************************************")
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    println("*************************************************")
    println("job:end")
    jobEnd.jobResult match {
      case JobSucceeded =>
        println("job:end:JobSucceeded")
      case JobFailed(exception) =>
        println("job:end:file")
        exception.printStackTrace()
    }
    println("*************************************************")
  }
}
