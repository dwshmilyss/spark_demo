package com.yiban.spark.rpc.dev

import java.util.concurrent.TimeUnit

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import org.apache.commons.lang3.time.StopWatch

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

object HelloworldClient {
  def main(args: Array[String]): Unit = {
    asyncCall()
    //    syncCall()
  }

  def asyncCall() = {
    val rpcEnv: RpcEnv = createRpcEnv("hello-client-config")
    val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress("localhost", 52345), "hello-service")
    val stopWatch : StopWatch = new StopWatch()
    stopWatch.start()
    val future: Future[String] = endPointRef.ask[String](SayHi("neo"))
    future.onComplete {
      case scala.util.Success(value) => println(s"Got the result = $value")
      case scala.util.Failure(e) => println(s"Got error: $e")
    }
    val res = Await.result(future, Duration.apply("15s"))
    println(s"$res")
    stopWatch.stop()
    println(s"cost time = ${stopWatch.getTime(TimeUnit.SECONDS)}s")
  }

  def syncCall() = {
    val rpcEnv: RpcEnv = createRpcEnv("hello-client-config")
    //获取要发送消息的RpcEndpointRef
    val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress("localhost", 52345), "hello-service")
    //    val result = endPointRef.askWithRetry[String](SayHi("neo"))
    val result = endPointRef.askWithRetry[String](SayBye("neo"))
    println(result)
  }

  private def createRpcEnv(configName: String): RpcEnv = {
    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, configName)
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
    rpcEnv
  }
}
