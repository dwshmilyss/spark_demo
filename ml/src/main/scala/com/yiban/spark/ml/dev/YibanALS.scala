package com.yiban.spark.ml.dev

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by 10000347 on 2015/7/28.
  * data 10.21.3.126 /root/data/cf
  */
object YibanALS {

  //加载用户评分数据
  def loadRatings(sc:SparkContext,path: String) : Seq[Rating] = {
    val ratings = sc.textFile(path).map { line =>
      val fields = line.split("::")
      Rating(fields(0).toInt,fields(1).toInt,fields(2).toDouble)
    }.filter(_.rating > 0.0)

    if (ratings.isEmpty){
      sys.error("No ratings provided")
    }else{
      ratings.toArray().toSeq
    }
  }

  /**
    * 校验预测数据和实际数据之间的均方差误差
    * @param model 训练数据集（即50%的数据）
    * @param data 校验数据集（即20%数据）
    * @param n 校验数据集的个数
    * @return 均方根误差值
    */
  def computeRmse(model:MatrixFactorizationModel,data:RDD[Rating],n:Long):Double = {
    //对训练数据集的结果进行预测
    val predictions : RDD[Rating] = model.predict(data.map(x => (x.user,x.product)))
    //训练数据集的预测结果跟实际的校验数据集进行join操作
    val predictionsAndRatings = predictions.map(x => ((x.user,x.product),x.rating))
      .join(data.map(x => ((x.user,x.product),x.rating)))
      .values
    //计算均方根误差
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }

  /**
    * 计算APK 预测模型好坏时要对每一个用户进行推荐后再计算每一个APK
    * @param actual
    * @param predicted
    * @param k
    * @return
    */
  def avgPrecisionK(actual:Seq[Int],predicted:Seq[Int],k:Int): Double ={
    val predK = predicted.take(k)
    var score = 0.0
    var numHits = 0.0
    for ((p,i) <- predK.zipWithIndex){
      if(actual.contains(p)){
        numHits += 1.0
        score += numHits / (i.toDouble + 1.0)
      }
    }
    if (actual.isEmpty){
      1.0
    }else{
      score / scala.math.min(actual.size,k).toDouble
    }
  }


  def main(args: Array[String]) {
    //设置日志级别
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("MovieLensALS")
    val sc = new SparkContext(conf)

    //加载电影数据,生成tuple，最后转换为map。(电影ID -> 电影标题)
    val movies = sc.textFile("hdfs://yiban.isilon:8020/ml/implict_item.dat").filter(x => !(x == "")).filter(x => x.split("::").size == 2).map {
      line => val fields = line.split("::")
        (fields(0).toInt,fields(1))
    }.collect().toMap

    //加载样本评分数据，其中最后一列timestamp对10取余数会作为key,Rating为value,即(Int,Rating)
    //   RDD[rand, Rating(user, obj, val)]
//    val sample_ratings = sc.textFile("hdfs://yiban.isilon:8020/ml/implict_data.dat").map {
//      line => val fields = line.split("::")
//        (fields(2).toLong % 10,Rating(fields(0).toInt,fields(1).toInt,1d))
//    }

    val sample_ratings = sc.textFile("hdfs://yiban.isilon:8020/ml/implict_data_modify.txt").map {
      line => val fields = line.split("::")
        ((Math.random*10).toInt, Rating(fields(0).toInt,fields(1).toInt,fields(2).toInt))
    }

//    val numRatings = sample_ratings.count()
//    val numUsers = sample_ratings.map(_._2.user).distinct().count()
//    val numMovies = sample_ratings.map(_._2.product).distinct().count()

    //样本数据以key切分成3个部分数据用于训练模型（key是对10取模的，所以值是0~9），分别是训练(60% 并加入用户评分)，校验(20%)，测试(20%)
    //下面的数据会在迭代计算中多次用到，所以需要cache
    val numPartitions = 4
    //训练集
    val training : RDD[Rating] = sample_ratings.filter(x => x._1 < 9 && x._1>=4).values.repartition(numPartitions).cache()
    //校验集
    val validation = sample_ratings.filter(x => x._1 < 2 && x._2.rating > 0 ).values.repartition(numPartitions).cache()
    //测试集
    val test = sample_ratings.filter(x => x._1 >= 2 && x._1 < 4 && x._2.rating > 0 ).values.cache()

    val numTraining = training.count()
    val numValidation = validation.count()
    val numTest = test.count()

    //训练不同参数下的模型，并在校验集中验证，获取最佳的模型
    val ranks = List(10,20) //特征个数
    val lambdas  = List(0.5,0.1,1.0) //正则化项
    val numIterations = List(10,20) //迭代次数
    val alphas = List(0.01,0.1,1.0)
    /*
        val ranks = List(20) //特征个数
        val lambdas  = List(1.0) //正则化项
        val numIterations = List(20) //迭代次数
        val alphas = List(1.0)
        */

    //初始化最优模型设置
    var bestModel : Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    //训练模型并进行交叉验证
    for (rank <- ranks;lambda <- lambdas;numIteration <- numIterations;alpha <- alphas){
      val model = ALS.trainImplicit(training,rank,numIteration,lambda,alpha)
      val validationRmse = computeRmse(model,validation,numValidation)
      println("RMSE(validation) = "+ validationRmse+" for the trained with rank = "+rank+"" +
        " lambda = "+lambda+" and numIteration = "+numIteration+" and alpha = "+alpha)
      if (validationRmse < bestValidationRmse){
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIteration
      }
    }

    //
    //    //用最佳模型预测测试数据集的评分，并计算和实际评分之间的均方根误差
    val testRmse = computeRmse(bestModel.get,test,numTest)
    println("the best model was trained with rank = "+bestRank+" and lambda = "+bestLambda+
      " and numIter = "+bestNumIter+" and its RMSE on the test set is "+testRmse)


    //给用户推荐
    val userId = 1219538
//    val userId = 5582115
    val K = 10
    val topKRecs = bestModel.get.recommendProducts(userId,K)
    topKRecs.foreach(println)
    bestModel.get.productFeatures.take(1)

    val itemsForUser = sample_ratings.keyBy(_._1).lookup(5582115)
//    val values = sample_ratings.keyBy(_._2.user).lookup(5582115)
    val values = sample_ratings.groupBy(_._2.user).map(x => (x._2.size,x._1)).sortByKey(false).map(x => (x._2,x._1))
    values.filter(x => x._2 == 39404).take(10)

//    values.foreach((x)=> {
//      val (r,rat) = x
//      if (rat.rating > 0) {
//        println(movies(rat.product))
//      }
//    })

    //    // 用户评过分的电影集合
    //    val myRateMovieIds = user_ratings.map(_.product).toArray().toSet
    //    //过滤用户已经评过分的电影
    //    val candidates = sc.parallelize(movies.keys.filter(!myRateMovieIds.contains(_)).toSeq)
    //    //利用训练处的最佳模型预测用户的偏好，按评分高低排序 取前10
    //    val recommendations = bestModel.get.predict(candidates.map((0,_))).collect().sortBy(-_.rating).take(10)
    //    var i = 1
    //    println("Movies recommended for you")
    //    recommendations.foreach {
    //      r => println("%2d".format(i)+" "+movies(r.product))
    //      i += 1
    //    }
    //
    sc.stop()
  }
}