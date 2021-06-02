package com.yiban.spark.rpc.dev
import java.util.concurrent._

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, RpcEnvClientConfig}

object PerformanceTestClient {
  def main(args: Array[String]): Unit = {
//    val host = args(0)
//    val invokeNumber = args(1).toInt
//    val concurrentNumber = args(2).toInt
    val host = "localhost"
    val invokeNumber = 100000
    val concurrentNumber = 50
    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, "hello-client")
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
    val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress(host, 52345), "hello-service")
    testConcurrentCall(invokeNumber, concurrentNumber, endPointRef)
  }

  def testConcurrentCall(invokeNum: Int, concurrentNumber: Int, endPointRef: RpcEndpointRef) = {
    val executors = Executors.newFixedThreadPool(concurrentNumber)
    try {
      val completionService = new ExecutorCompletionService[Long](executors)
      for (i <- 1 to invokeNum) {
        completionService.submit(new Caller(endPointRef))
      }
      var elapsedTime = 0L
      val starting = System.currentTimeMillis()
      for (i <- 1 to invokeNum) {
        val future: Future[Long] = completionService.take()
        elapsedTime = elapsedTime + future.get()
      }
      val cost = System.currentTimeMillis() - starting
      println(s"Total used time (s): ${TimeUnit.SECONDS.convert(cost,TimeUnit.MILLISECONDS)}")
      println(s"Average cost time (ms): ${TimeUnit.MILLISECONDS.convert(elapsedTime / invokeNum,TimeUnit.NANOSECONDS)}")
      println(s"QPS: ${1000.0 / ((cost * 1.0) / invokeNum).toInt}")
    } finally {
      executors.shutdown()
    }
  }


  class Caller(endPointRef: RpcEndpointRef) extends Callable[Long] {
    override def call(): Long = {
      val beginning = System.nanoTime()
      endPointRef.askWithRetry[String](SayBye("neo"))
      val ending = System.nanoTime()
      ending - beginning
    }
  }
}
