package com.yiban.spark.core.dev
import org.apache.spark.{AccumulableParam, SparkConf, SparkContext}

object FunctionDemo {
  val conf = new SparkConf().setAppName("FunctionDemo").setMaster("local")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
//    testCombineByKey()
//    testAggregateByKey()
    testAggregate()
    sc.stop()
  }

  /**
    *
    * a 、score => (1, score)，我们把分数作为参数,并返回了附加的元组类型。 以"Fred"为列，当前其分数为88.0 =>(1,88.0)  1表示当前科目的计数器，此时只有一个科目
    b、(c1: MVType, newScore) => (c1._1 + 1, c1._2 + newScore)，注意这里的c1就是createCombiner初始化得到的(1,88.0)。在一个分区内，我们又碰到了"Fred"的一个新的分数91.0。当然我们要把之前的科目分数和当前的分数加起来即c1._2 + newScore,然后把科目计算器加1即c1._1 + 1
    c、 (c1: MVType, c2: MVType) => (c1._1 + c2._1, c1._2 + c2._2)，注意"Fred"可能是个学霸,他选修的科目可能过多而分散在不同的分区中。所有的分区都进行mergeValue后,接下来就是对分区间进行合并了,分区间科目数和科目数相加分数和分数相加就得到了总分和总科目数
    */
  def testCombineByKey(): Unit ={
    val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))
    val d1 = sc.parallelize(initialScores)
    type MVType = (Int, Double) //定义一个元组类型(科目计数器,分数)
    d1.combineByKeyWithClassTag(
      score => (1, score),
      (c1: MVType, newScore) => (c1._1 + 1, c1._2 + newScore),
      (c1: MVType, c2: MVType) => (c1._1 + c2._1, c1._2 + c2._2)
    ).map { case (name, (num, socre)) => (name, socre / num) }.collect.foreach(println)
  }



  def testAggregateByKey(): Unit ={
    val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))
    val d1 = sc.parallelize(initialScores)
    type MVType = (Int, Double) //定义一个元组类型(科目计数器,分数)
    val rdd = d1.aggregateByKey((0,0d))((c1: MVType, newScore:Double) => (c1._1 + 1, c1._2.toDouble + newScore),(c1: MVType, c2: MVType) => (c1._1 + c2._1, c1._2 + c2._2))
    rdd.map { case (name, (num, socre)) => (name, socre / num) }.collect.foreach(println)
  }


  def testAggregate(): Unit ={
    val rdd = sc.parallelize(List(1,2,3,4,5,6,7,8,9),3)

    def seqOp(acc:(Int,Int),b:Int):(Int,Int) = {
      println("SeqOp:"+(acc._1+"+"+b)+"\t"+(acc._2+"+"+1))
      (acc._1+b,acc._2+1)
    }

    def combOp(acc1:(Int,Int),acc2:(Int,Int)):(Int,Int) = {
      println("combOp:"+(acc1._1+acc2._1)+"\t"+(acc1._2+acc2._2))
      (acc1._1+acc2._1,acc1._2+acc2._2)
    }

    val res  = rdd.aggregate((0,0))(seqOp,combOp)
    println(res)

  }

  def test(): Unit ={
    val sparkConf: SparkConf = new SparkConf().setAppName("AggregateByKey").setMaster("local")
    val sc: SparkContext = new SparkContext(sparkConf)

    val data=List((1,3),(1,2),(1,4),(2,3))
    val rdd=sc.parallelize(data, 2)

    //合并不同partition中的值，a，b得数据类型为zeroValue的数据类型
    def combOp(a:String,b:String):String={
      println("combOp: "+a+"\t"+b)
      a+b
    }
    //合并在同一个partition中的值，a的数据类型为zeroValue的数据类型，b的数据类型为原value的数据类型
    def seqOp(a:String,b:Int):String={
      println("SeqOp:"+a+"\t"+b)
      a+b
    }
    rdd.foreach(println)
    //zeroValue:中立值,定义返回value的类型，并参与运算
    //seqOp:用来在同一个partition中合并值
    //combOp:用来在不同partiton中合并值
    val aggregateByKeyRDD=rdd.aggregateByKey("100")(seqOp, combOp)
    sc.stop()
  }

  def testCombine(): Unit ={

  }
}
