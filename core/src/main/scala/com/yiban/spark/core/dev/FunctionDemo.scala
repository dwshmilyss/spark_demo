package com.yiban.spark.core.dev

import org.apache.spark.{SparkConf, SparkContext}

object FunctionDemo {
  val conf = new SparkConf().setAppName("FunctionDemo").setMaster("local")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    //    testCombineByKey()
    //    testAggregateByKey()
    testAggregateAndTreeAggregate()
    sc.stop()
  }

  /**
    *
    * a 、score => (1, score)，我们把分数作为参数,并返回了附加的元组类型。 以"Fred"为列，当前其分数为88.0 =>(1,88.0)  1表示当前科目的计数器，此时只有一个科目
    * b、(c1: MVType, newScore) => (c1._1 + 1, c1._2 + newScore)，注意这里的c1就是createCombiner初始化得到的(1,88.0)。在一个分区内，我们又碰到了"Fred"的一个新的分数91.0。当然我们要把之前的科目分数和当前的分数加起来即c1._2 + newScore,然后把科目计算器加1即c1._1 + 1
    * c、 (c1: MVType, c2: MVType) => (c1._1 + c2._1, c1._2 + c2._2)，注意"Fred"可能是个学霸,他选修的科目可能过多而分散在不同的分区中。所有的分区都进行mergeValue后,接下来就是对分区间进行合并了,分区间科目数和科目数相加分数和分数相加就得到了总分和总科目数
    */
  def testCombineByKey(): Unit = {
    val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))
    val d1 = sc.parallelize(initialScores)
    type MVType = (Int, Double) //定义一个元组类型(科目计数器,分数)
    d1.combineByKeyWithClassTag(
      score => (1, score),
      (c1: MVType, newScore) => (c1._1 + 1, c1._2 + newScore),
      (c1: MVType, c2: MVType) => (c1._1 + c2._1, c1._2 + c2._2)
    ).map { case (name, (num, socre)) => (name, socre / num) }.collect.foreach(println)
  }


  def testAggregateByKey(): Unit = {
    val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))
    val d1 = sc.parallelize(initialScores)
    type MVType = (Int, Double) //定义一个元组类型(科目计数器,分数)
    val rdd = d1.aggregateByKey((0, 0d))((c1: MVType, newScore: Double) => (c1._1 + 1, c1._2.toDouble + newScore), (c1: MVType, c2: MVType) => (c1._1 + c2._1, c1._2 + c2._2))
    rdd.map { case (name, (num, socre)) => (name, socre / num) }.collect.foreach(println)
  }


  /**
    * seqOp 在当前分区中进行 （第一个参数就是zeroValue，第二个参数是rdd中的数据，每次执行完的结果就是下次seqOp的第一个参数）
    * combOp 在所有分区上进行 （第一个参数是zeroValue，第二个参数是每个分区seqOp的返回值，每次通过zeroValue和seqOp的返回值进行combOp运算后得到下一次combOp的第一个参数）
    * 返回值类型和zeroValue类型一致
    */
  def testAggregate(): Unit = {
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)

    def seqOp(acc: (Int, Int), b: Int): (Int, Int) = {
      println("SeqOp:" + (acc._1 + "+" + b) + "\t" + (acc._2 + "+" + 1))
      (acc._1 + b, acc._2 + 1)
    }

    def combOp(acc1: (Int, Int), acc2: (Int, Int)): (Int, Int) = {
      println("combOp:" + (acc1._1 + acc2._1) + "\t" + (acc1._2 + acc2._2))
      (acc1._1 + acc2._1, acc1._2 + acc2._2)
    }

    val res = rdd.aggregate((0, 0))(seqOp, combOp)
    println(res)
  }

  /**
    * 查看aggregate的代码和treeAggregate的代码实现会发现，确实如上现象所反映，整理结果如下：
      （1）最终结果上，aggregate会比treeAggregate多做一次对于初始值的combOp操作。但从参数名字上就可以看到，
          一般要传入类似0或者空的集合的zeroValue初始值。
      （2）aggregate会把分区的结果直接拿到driver端做reduce操作。treeAggregate会先把分区结果做reduceByKey，
          最后再把结果拿到driver端做reduce,算出最终结果。reduceByKey需要几层，由参数depth决定，也就是相当于
          做了depth层的reduceByKey，这也是treeAggregate名字的由来。

    优缺点
      （1） aggregate在combine上的操作，复杂度为O(n). treeAggregate的时间复杂度为O(lg n)。n为分区数。
       (2) aggregate把数据全部拿到driver端，存在内存溢出的风险。treeAggregate则不会。
    */
  def testAggregateAndTreeAggregate(): Unit = {
    def seqOp(a: Int, b: Int): Int = {
      println("seq:" + a + ":" + b)
      math.min(a, b)
    }

    def combOp(a: Int, b: Int): Int = {
      println("comb:" + a + ":" + b)
      a + b
    }

    val z =sc.parallelize(List(1,2,4,5,8,9),3)

    //结果是10 因为初始值3参与了combOp
    z.aggregate(3)(seqOp,combOp)
    //结果是7 因为初始值3并没有参与combOp
    z.treeAggregate(3)(seqOp,combOp)
  }

  def test(): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("AggregateByKey").setMaster("local")
    val sc: SparkContext = new SparkContext(sparkConf)

    val data = List((1, 3), (1, 2), (1, 4), (2, 3))
    val rdd = sc.parallelize(data, 2)

    //合并不同partition中的值，a，b得数据类型为zeroValue的数据类型
    def combOp(a: String, b: String): String = {
      println("combOp: " + a + "\t" + b)
      a + b
    }
    //合并在同一个partition中的值，a的数据类型为zeroValue的数据类型，b的数据类型为原value的数据类型
    def seqOp(a: String, b: Int): String = {
      println("SeqOp:" + a + "\t" + b)
      a + b
    }
    rdd.foreach(println)
    //zeroValue:中立值,定义返回value的类型，并参与运算
    //seqOp:用来在同一个partition中合并值
    //combOp:用来在不同partiton中合并值
    val aggregateByKeyRDD = rdd.aggregateByKey("100")(seqOp, combOp)
    sc.stop()
  }

  def testCombine(): Unit = {

  }
}
