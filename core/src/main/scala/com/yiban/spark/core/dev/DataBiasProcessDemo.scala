package com.yiban.spark.core.dev

import java.util
import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer


/**
  * 处理数据倾斜的方法：
  * 解决方案一：使用Hive ETL预处理数据，该方法只是将数据倾斜的步骤提前到数据源(HIVE或者HDFS)阶段，通过ETL对倾斜的数据进行聚合分组等操作。实际作用不大
  * 解决方案二：过滤少数导致倾斜的key。如果只有极少数几个倾斜的KEY，那么可以过滤掉这几个KEY。
  * 解决方案三：提高shuffle操作的并行度。例如reduceByKey(func,numPartitions)重新指定分区数，或者利用配置spark.default.parallelism=n
  */
object DataBiasProcessDemo {

  val conf = new SparkConf().setMaster("local[*,4]").setAppName("accumulator")
  val sc = new SparkContext(conf)
  val rdd1 = sc.parallelize(List(("hello", 1), ("hello", 2), ("hello", 3), ("hello", 4), ("hello", 5), ("dw", 1), ("dw", 2), ("dw", 3), ("dw", 4)))
  val rdd2 = sc.parallelize(List(("hello", 1), ("hello", 2), ("dw", 3), ("dw", 4)))
  val random = new Random(1000)

  /**
    * 解决方案四：两阶段聚合(局部聚合+全局聚合)
    */
  def doProcess1() = {
    // 第一步，给RDD中的每个key都打上一个随机前缀。
    val randomPrefixRDD = rdd1.map {
      x =>
        val prefix = random.nextInt(10)
        (prefix + "_" + x._1, x._2)
    }
    // 第二步，对打上随机前缀的key进行局部聚合。
    val localAggrRDD = randomPrefixRDD.reduceByKey(_ + _)
    // 第三步，去除RDD中每个key的随机前缀。
    val removedRandomPrefixRDD = localAggrRDD.map { x =>
      (x._1.split("_")(1), x._2)
    }
    // 第四步，对去除了随机前缀的RDD进行全局聚合。
    removedRandomPrefixRDD.reduceByKey(_ + _)
  }

  /**
    * 数据集一个大一个小
    * 解决方案五：将reduce join转为map join
    * 利用广播变量broadcast
    */
  def doProcess2 = {
    //广播较小的数据集
    val rdd2Data = rdd2.collect()
    val rdd2DataBroadcast = sc.broadcast(rdd2Data)

    val joinedRDD = rdd1.map { x =>
      //在每个executor上获取广播的数据
      val rdd2Data = rdd2DataBroadcast.value
      //可以将该数据转换成map 这样检索会很快  但这只适用于key没有重复的情况
      val map = new util.HashMap[String, Int]()
      for ((key, value) <- rdd2Data) {
        map.put(key, value)
      }
      val (key, value) = x
      val rdd2Value = map.get(key)
      (key, (value, rdd2Value))
    }
  }

  /**
    * 如果两个数据集都很大 但是倾斜的key很少
    * 解决方案六：采样倾斜key并分拆join操作
    */
  def doProcess3 = {
    //首先从包含了少数几个导致数据倾斜key的rdd1中，采样10%的样本数据。
    val sampleRDD = rdd1.sample(false, 0.1)
    val mappedSampledRDD = sampleRDD.map(x => (x._1, 1))
    //对样本数据RDD统计出每个key的出现次数
    val countedSampledRDD = mappedSampledRDD.reduceByKey(_ + _)
    //并按出现次数降序排序。对降序排序后的数据，取出top1或者top100的数据，也就是key最多的前n个数据。
    //具体取出多少个数据量最多的key，由大家自己决定，我们这里就取1个作为示范。
    val skwewdRDD1Key = countedSampledRDD.map(x => (x._2, x._1)).sortByKey(false).take(1)(0)._2
    //从rdd1中分拆出导致数据倾斜的key，形成独立的RDD。
    val skewedRDD1 = rdd1.filter(x => (x._1.equals(skwewdRDD1Key)))
    //从rdd1中分拆出不导致数据倾斜的普通key，形成独立的RDD。
    val commonRDD1 = rdd1.filter(x => (!x._1.equals(skwewdRDD1Key)))



    //rdd2，就是那个所有key的分布相对较为均匀的rdd。
    //这里将rdd2中，前面获取到的key对应的数据，过滤出来，分拆成单独的rdd，
    //并对rdd中的数据使用flatMap算子都扩容100倍。对扩容的每条数据，都打上0～100的前缀。 即每个key膨胀100倍
    val skewedRdd2 = rdd2.filter(x => (x._1.equals(skwewdRDD1Key))).flatMap{
      x =>
      var list = new scala.collection.mutable.ListBuffer[(String,Int)]
        for (i <- 0 until 100){
          list.+=((i+"_"+x._1,x._2))
        }
        list
    }

    //将rdd1中分拆出来的导致倾斜的key的独立rdd，每条数据都打上100以内的随机前缀。
    //然后将这个rdd1中分拆出来的独立rdd，与上面rdd2中分拆出来的独立rdd，进行join。
    val joinedRDD1 = skewedRDD1.map{
      x =>
        val random = new Random(100)
        var prefix = random.nextInt(100)
        (prefix+"_"+x._1,x._2)
    }.join(skewedRdd2).map{
      x =>
        val key = x._1.split("_")(1)
        (key,x._2)
    }
    //将rdd1中分拆出来的包含普通key的独立rdd，直接与rdd2进行join。
    val joinedRDD2 = commonRDD1.join(rdd2)
    //将倾斜key join后的结果与普通key join后的结果，uinon起来。
    //就是最终的join结果。
    val joinedRDD = joinedRDD1.union(joinedRDD2)
  }

  /**
    * 如果两个数据集都很大  而且倾斜的key也很多
    * 解决方案七：使用随机前缀和扩容RDD进行join。和方案六基本一致
    * //首先将其中一个key分布相对较为均匀的RDD膨胀100倍。
    * //其次，将另一个有数据倾斜key的RDD，每条数据都打上100以内的随机前缀。
    * //将两个处理后的RDD进行join即可。
    */

    //解决方案八：多种方案组合使用
}
