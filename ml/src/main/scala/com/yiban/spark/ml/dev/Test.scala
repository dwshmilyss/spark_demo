package com.yiban.spark.ml.dev

/**
  * Created by 10000347 on 2016/4/11.
  */
object Test {

//  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
//  case class MyRating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
//  object RatingLoad {
//    def parseRating(str: String): MyRating = {
//      val fields = str.split("::")
//      MyRating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
//    }
//  }
//
//  object RatingImplict {
//    def parseRating(ratingObj: MyRating): MyRating = {
//      if (ratingObj.rating > 3f) {
//        MyRating(ratingObj.userId,ratingObj.movieId,1f,ratingObj.timestamp)
//      }
//      else {
//        MyRating(ratingObj.userId,ratingObj.movieId,0f,ratingObj.timestamp)
//      }
//    }
//  }
//  val conf = new SparkConf().setAppName("ALSExample")
//  val sc = new SparkContext(conf)
//  val sqlContext = new SQLContext(sc)
//  import sqlContext.implicits._
//
////  val ratings = sc.textFile("hdfs://yiban.isilon:8020/ml/sample_movielens_ratings.txt").map(RatingLoad.parseRating).toDF()
//  val ratings = sc.textFile("hdfs://yiban.isilon:8020/ml/sample_movielens_ratings.txt").map(RatingLoad.parseRating).map(RatingImplict.parseRating).toDF()
//  val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
//  // Build the recommendation model using ALS on the training data
//  val als = new ALS().setMaxIter(10).setRegParam(0.1).setRank(20).setAlpha(0.01).setUserCol("userId").setItemCol("movieId").setRatingCol("rating")
//  val model = als.setImplicitPrefs(true).fit(training)
//  // Evaluate the model by computing the RMSE on the test data
//  val predictions = model.transform(test).withColumn("rating", col("rating").cast(DoubleType)).withColumn("prediction", col("prediction").cast(DoubleType))
//  val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")
//  val rmse = evaluator.evaluate(predictions)
//  println(s"Root-mean-square error = $rmse")
}
