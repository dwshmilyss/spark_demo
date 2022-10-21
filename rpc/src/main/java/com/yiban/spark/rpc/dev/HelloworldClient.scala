package com.yiban.spark.rpc.dev

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

import scala.concurrent.ExecutionContext.Implicits.global

object HelloworldClient {
  def main(args: Array[String]): Unit = {
    //    asyncCall()
    syncCall()
  }

  def asyncCall() = {
    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, "hello-client")
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
    val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress("localhost", 52345), "hello-service")
    val future: Future[String] = endPointRef.ask[String](SayHi("neo"))
    future.onComplete {
      case scala.util.Success(value) => println(s"Got the result = $value")
      case scala.util.Failure(e) => println(s"Got error: $e")
    }
    Await.result(future, Duration.apply("30s"))
  }

  def syncCall() = {
    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, "hello-client")
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
    val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress("localhost", 52345), "hello-service")
    val result = endPointRef.askWithRetry[String](SayHi("neo"))
    println(result)
  }
}
