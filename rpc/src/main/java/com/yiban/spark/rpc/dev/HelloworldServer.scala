package com.yiban.spark.rpc.dev

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc._
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory

object HelloworldServer {
  def main(args: Array[String]): Unit = {
    //    val host = args(0)
    val host = "localhost"
    val config = RpcEnvServerConfig(new RpcConf(), "hello-server", host, 52345)
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
    val helloEndpoint: RpcEndpoint = new HelloEndpoint(rpcEnv)
    //服务端必须启动EndpointRef 客户端才能和其通信
    val serverEndpointRef = rpcEnv.setupEndpoint("hello-service", helloEndpoint)
    //TODO test 自己向自己发送消息
    //    val res = serverEndpointRef.askWithRetry[String](SayBye("neo"))
    //    println(res)
    rpcEnv.awaitTermination()
  }
}

class HelloEndpoint(override val rpcEnv: RpcEnv) extends RpcEndpoint {

  override def onStart(): Unit = {
    println("start hello endpoint")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case SayHi(msg) => {
      //println(s"receive $msg")
      Thread.sleep(10*1000)
      context.reply(s"hi, $msg")
    }
    case SayBye(msg) => {
      //println(s"receive $msg")
      context.reply(s"bye, $msg")
    }
  }

  override def onStop(): Unit = {
    println("stop hello endpoint")
  }
}


case class SayHi(msg: String)

case class SayBye(msg: String)