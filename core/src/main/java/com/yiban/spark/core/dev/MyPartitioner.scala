package com.yiban.spark.core.dev

import java.net.URL

import org.apache.spark.Partitioner

/**
  * 把来自同一个域名的URL放到一台节点上，比如:https://www.iteblog.com和https://www.iteblog.com/archives/1368
  * 如果直接使用HashPartitioner 则根据string进行hashcode，这2个URL的hashcode肯定不一样，就会被分配到不同的partition里面
  * 如果两个pairRDD进行join 则按照相同的key划分partition会大大的减少shuffle
  * 注意：如果
  *
  * 1.partitionBy之后一定要使用persist()将partitionBy的结构保存下来,否则下次使用的时候还会重新计算依赖链,(官方说的,我这里还不确定,持怀疑态度).
    2.map等普通RDD的操作会将rdd中partitioner属性设置为None，因为map等操作能够改变key的类型。
    3.对join效率的提高,不一定要用HashPartitioner,使用RangePartitioner也行这和Partitioner的类型无关.
    4.在使用到partitioner的方法中，如果没有显示的指明partitioner的类型，会调用Partitioner类的defaultPartitioner0方法，查看这个方法的代码会发现，如果rdd已经设置了partitioner的信息，spark.default.parallelism这个属性会失效。
    5.HashPartitioner(num)，num决定了shuffle过程的并发量。
    6.不要使用Array类型作为key-value数据的key,HashPartitioner不能以Array为Key进行分区.
  *
  *
  * 在Python中，你不需要扩展Partitioner类，我们只需要对iteblog.partitionBy()加上一个额外的hash函数，如下：
  * import urlparse

    def iteblog_domain(url):
      return hash(urlparse.urlparse(url).netloc)

    iteblog.partitionBy(20, iteblog_domain)


  * @param numParts
  */
class MyPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    val domain = new URL(key.toString).getHost
    val code = (domain.hashCode % numPartitions)
    //对负值进行处理
    if (code < 0) {
      code + numPartitions
    } else {
      code
    }
  }

  override def equals(other: Any): Boolean = other match {
    case myPartitioner: MyPartitioner =>
      myPartitioner.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}
