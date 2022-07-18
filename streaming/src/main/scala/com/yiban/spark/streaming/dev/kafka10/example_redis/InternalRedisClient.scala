package com.yiban.spark.streaming.dev.kafka10.example_redis

import org.apache.commons.pool.impl.GenericObjectPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object InternalRedisClient extends Serializable {
  @transient private var pool:JedisPool = null

  def makePool(redisHost:String,redisPort:Int,redisTimeout:Int,maxTotal:Int,maxIdle:Int,minIdle:Int) : Unit = {
    makePool(redisHost, redisPort, redisPort, maxTotal, maxIdle, minIdle, true, false, 10000)
  }

  def makePool(redisHost:String,redisPort:Int,redisTimeout:Int,maxTotal:Int,maxIdle:Int,minIdle:Int,testOnBorrow:Boolean,testOnReturn:Boolean,maxWaitMillis:Long):Unit = {
    if (pool == null) {
      val poolConfig = new GenericObjectPoolConfig()
      poolConfig.setMaxTotal(maxTotal)
      poolConfig.setMaxIdle(maxIdle)
      poolConfig.setMinIdle(minIdle)
      poolConfig.setTestOnBorrow(testOnBorrow)
      poolConfig.setTestOnReturn(testOnReturn)
      pool = new JedisPool(poolConfig,redisHost,redisPort,redisTimeout)

      val hook = new Thread{
        override def run = pool.destroy()
      }
      sys.addShutdownHook(hook.run)
    }
  }

  def getPool:JedisPool = {
    assert(pool != null)
    pool
  }
}
