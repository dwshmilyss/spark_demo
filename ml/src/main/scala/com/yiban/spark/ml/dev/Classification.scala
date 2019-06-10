package com.yiban.spark.ml.dev

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{ClassificationModel, LogisticRegressionWithSGD, NaiveBayes, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.optimization.{SimpleUpdater, SquaredL2Updater, Updater}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.{Entropy, Gini, Impurity}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 10000347 on 2016/4/18.
  */
object Classification {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  val conf = new SparkConf().setAppName("Classification")
  val sc = new SparkContext(conf)

  def preProcessData(): Unit = {
    val rawData = sc.textFile("hdfs://yiban.isilon:8020/ml/kaggle/train_no_header.tsv")
    val records = rawData.map(line => line.split("\t"))
//    records.first()
    val data = records.map { r =>
      val trimmed = r.map(_.replaceAll("\"",""))
      val label = trimmed(r.size - 1).toInt
      val features = trimmed.slice(4,r.size - 1).map(d => if (d == "?") 0.0 else d.toDouble)
      LabeledPoint(label,Vectors.dense(features))
    }
    //nvtive bayes要使用没有负特征值的数据
    val nbData = records.map { r =>
      val trimmed = r.map(_.replaceAll("\"",""))
      val label = trimmed(r.size - 1).toInt
      val features = trimmed.slice(4,r.size - 1).map(d => if (d == "?") 0.0 else d.toDouble).map(d => if (d<0) 0.0 else d)
      LabeledPoint(label,Vectors.dense(features))
    }
    data.cache()
    nbData.cache()
    //迭代次数
    val numIter = 10;
    //训练模型
    //逻辑斯蒂回归模型
    val lrModel = LogisticRegressionWithSGD.train(data,numIter)
    //SVM模型
    val svmModel = SVMWithSGD.train(data,numIter)
    //native bayes 模型
    val nbModel = NaiveBayes.train(nbData)
    //decision tree 模型
    val maxTreeDepth = 5
    //Entropy不纯度估计
    val dtModel = DecisionTree.train(data,Algo.Classification,Entropy,maxTreeDepth)

    //使用模型
    val prediction = lrModel.predict(data.map(lp => lp.features)).take(5) //预测的结果（取前5个）
    //实际的结果
    val dataPoint = data.take(5)
    val trueLabel = dataPoint.map(lp => lp.label)

    //计算准确率
    //计算logisticRegression模型的准确率
    val lrTotalCorrect = data.map {
      point =>
        if (lrModel.predict(point.features) == point.label) 1 else 0
    }.sum()
    val lrAccuracy = lrTotalCorrect / data.count()
    //SVM的准确率
    val svmTotalCorrect = data.map {
      point =>
        if (svmModel.predict(point.features) == point.label) 1 else 0
    }.sum()
    val svmAccuracy = svmTotalCorrect / data.count()
    //native bayes
    val nbTotalCorrect = nbData.map {
      point =>
        if (nbModel.predict(point.features) == point.label) 1 else 0
    }.sum()
    val nbAccuracy = nbTotalCorrect / nbData.count()
    //decision tree
    val dtTotalCorrect = data.map {
      point =>
        val score = dtModel.predict(point.features)
        val predicted = if(score > 0.5) 1 else 0
        if (predicted == point.label) 1 else 0
    }.sum()
    val dtAccuracy = dtTotalCorrect / data.count()

    //计算各个模型的ROC曲线密度和PR值
    //lr和svm
    val lrMetrics = Seq(lrModel).map {
      model =>
        val scoreAndLabels = data.map(point => (model.predict(point.features),point.label))
        val metrics = new BinaryClassificationMetrics(scoreAndLabels)
        (model.getClass.getSimpleName,metrics.areaUnderPR(),metrics.areaUnderROC(),lrAccuracy)
    }
    val svmMetrics = Seq(svmModel).map {
      model =>
        val scoreAndLabels = data.map(point => (model.predict(point.features),point.label))
        val metrics = new BinaryClassificationMetrics(scoreAndLabels)
        (model.getClass.getSimpleName,metrics.areaUnderPR(),metrics.areaUnderROC(),svmAccuracy)
    }
    //native bayes
    val nbMetrics = Seq(nbModel).map {
      model =>
        val scoreAndLabels = data.map { point =>
          val score = model.predict(point.features)
          (if (score > 0.5) 1.0 else 0.0,point.label)
        }
        val metrics = new BinaryClassificationMetrics(scoreAndLabels)
        (model.getClass.getSimpleName,metrics.areaUnderPR(),metrics.areaUnderROC(),nbAccuracy)
    }
    //decision tree
    val dtMetrics = Seq(dtModel).map {
      model =>
        val scoreAndLabels = data.map {
          point =>
            val score = model.predict(point.features)
            (if (score > 0.5) 1.0 else 0.0,point.label)
        }
        val metrics = new BinaryClassificationMetrics(scoreAndLabels)
        (model.getClass.getSimpleName,metrics.areaUnderPR(),metrics.areaUnderROC(),dtAccuracy)
    }
    val allMetrics = lrMetrics ++ svmMetrics ++ nbMetrics ++ dtMetrics
    /**
     * 输出结果：
     * LogisticRegressionModel,Accuracy:51.4672%,Area under PR:75.6759%,Area under ROC:50.1418%
     * SVMModel,Accuracy:51.4672%,Area under PR:75.6759%,Area under ROC:50.1418%
     * NaiveBayesModel,Accuracy:58.0392%,Area under PR:68.1003%,Area under ROC:58.3683%
     * DecisionTreeModel,Accuracy:64.8276%,Area under PR:74.3081%,Area under ROC:64.8837%
     */
    allMetrics.foreach{
      case (m,pr,roc,accuracy) => println(f"$m,Accuracy:${accuracy * 100}%2.4f%%,Area under PR:${pr * 100.0}%2.4f%%,Area under ROC:${roc * 100.0}%2.4f%%")
    }

    //改进模型性能以及参数调优
    //1/ 特征标准化
    val vectors = data.map(lp => lp.features)
    //每个特征向量是矩阵的一行
    val matrix = new RowMatrix(vectors)
    //计算矩阵的列属性
    val matrixSummary = matrix.computeColumnSummaryStatistics()
    //每一列的均值
    println(matrixSummary.mean)
    //每一列的最大值
    println(matrixSummary.max)
    //每一列的最小值
    println(matrixSummary.min)
    //每一列的方差
    println(matrixSummary.variance)
    //每一列非0项的数目
    println(matrixSummary.numNonzeros)

    //标准化特征 对每个特征值减去列的均值 再除以列的标准差
    val scaler = new StandardScaler(withStd = true, withMean = true).fit(vectors)
    val scaledData = data.map(lp => LabeledPoint(lp.label,scaler.transform(lp.features)))
    //对比标准化前后的特征值
    println(data.first().features)
    println(scaledData.first().features)

    //重新用logistic regression和标准化后的数据训练模型 提高模型预测的准确率
    //其实就是对特征值正则化 使之分布服从正态分布
    val lrModelScaled = LogisticRegressionWithSGD.train(scaledData,numIter)
    val lrTotalCorrectScaled = scaledData.map {
      point =>
        if (lrModelScaled.predict(point.features) == point.label) 1 else 0
    }.sum()
    val lrAccuracyScaled = lrTotalCorrectScaled / scaledData.count()
    val lrPredictionsVsTrue = scaledData.map{
      point =>
        (lrModelScaled.predict(point.features),point.label)
    }
    val lrMetricsScaled = new BinaryClassificationMetrics(lrPredictionsVsTrue)
    val lrPr = lrMetricsScaled.areaUnderPR()
    val lrRoc = lrMetricsScaled.areaUnderROC()

    /**
      * 输出结果（可以和之前未标准化特征值的数据拟合的结果做对比）：
      * LogisticRegressionModel,Accuracy:62.0419%,Area under PR:72.7254%,Area under ROC:61.9663%
      */
    println(f"${lrModelScaled.getClass.getSimpleName},Accuracy:${lrAccuracyScaled * 100}%2.4f%%,Area under PR:${lrPr * 100.0}%2.4f%%,Area under ROC:${lrRoc * 100.0}%2.4f%%")

    /**
      * 针对train加入类别特征数据，重新训练模型拟合数据，提升模型效率
      */
    // 使用类别特征
    val categories = records.map(r => r(3)).distinct().collect().zipWithIndex.toMap
    //获取有多少个类别
    val numCategories = categories.size
    //使用1-of-k的方法创建向量并复制，类别索引所在的分量赋值为1 其他分量为0
    val dataCategories = records.map{
      r =>
        val trimmed = r.map(_.replaceAll("\"",""))
        val label = trimmed(r.size - 1).toInt
        val categoryIndex = categories(r(3))
        //创建一维数组 数组的长度为numCategories，如果参数是2个则为二维数组，以此类推
        val categoryFeatures = Array.ofDim[Double](numCategories)
        categoryFeatures(categoryIndex) = 1.0
        val otherFeatures = trimmed.slice(4,r.size - 1).map(d => if (d == "?") 0.0 else d.toDouble)
        val features = categoryFeatures ++ otherFeatures
        LabeledPoint(label,Vectors.dense(features))
    }
    //对原始特征值进行标准化转换
    val scalerCats = new StandardScaler(withMean = true,withStd = true).fit(dataCategories.map(lp => lp.features))
    val scaledDataCats = dataCategories.map(lp => LabeledPoint(lp.label,scalerCats.transform(lp.features)))
    //利用新的特征值重新训练模型（还是以逻辑斯蒂回归为例）
    val lrModelScaledCats = LogisticRegressionWithSGD.train(scaledDataCats,numIter)
    val lrTotalCorrectScaledCats = scaledDataCats.map {
      point =>
        if (lrModelScaledCats.predict(point.features) == point.label) 1 else 0
    }.sum()
    val lrAccuracyScaledCats = lrTotalCorrectScaledCats / scaledDataCats.count()
    val lrPredictionsVsTrueCats = scaledDataCats.map{
      point =>
        (lrModelScaledCats.predict(point.features),point.label)
    }
    val lrMetricsScaledCats = new BinaryClassificationMetrics(lrPredictionsVsTrueCats)
    val lrPrCats = lrMetricsScaledCats.areaUnderPR()
    val lrRocCats = lrMetricsScaledCats.areaUnderROC()

    /**
      * 输出结果（可以和之前标准化特征值的数据拟合的结果做对比，可以看到Accuracy从62%提升到66%）：
      * LogisticRegressionModel,Accuracy:66.5720%,Area under PR:75.7964%,Area under ROC:66.5483%
      */
    println(f"${lrModelScaledCats.getClass.getSimpleName},Accuracy:${lrAccuracyScaledCats * 100}%2.4f%%,Area under PR:${lrPrCats * 100.0}%2.4f%%,Area under ROC:${lrRocCats * 100.0}%2.4f%%")

    // 1-of-k类型的特征值更适合朴素贝叶斯模型
    val dataNB = records.map{
      r =>
        val trimmed = r.map(_.replaceAll("\"",""))
        val label = trimmed(r.size - 1).toInt
        val categoryIndex = categories(r(3))
        //创建一维数组 数组的长度为numCategories，如果参数是2个则为二维数组，以此类推
        val categoryFeatures = Array.ofDim[Double](numCategories)
        categoryFeatures(categoryIndex) = 1.0
        LabeledPoint(label,Vectors.dense(categoryFeatures))
    }
    //利用新的特征值重新训练模型（以朴素贝叶斯为例）
    val nbModelCats = NaiveBayes.train(dataNB)
    val nbTotalCorrectCats = dataNB.map {
      point =>
        if (nbModelCats.predict(point.features) == point.label) 1 else 0
    }.sum()
    val nbAccuracyCats = nbTotalCorrectCats / dataNB.count()
    val nbPredictionsVsTrueCats = dataNB.map{
      point =>
        (nbModelCats.predict(point.features),point.label)
    }
    val nbMetricsCats = new BinaryClassificationMetrics(nbPredictionsVsTrueCats)
    val nbPrCats = nbMetricsCats.areaUnderPR()
    val nbRocCats = nbMetricsCats.areaUnderROC()

    /**
      * 合适的特征值可以提高模型的准确度（可以和最开始的bayes模型做对比，模型准确度由58%提升到60%）
      * NaiveBayesModel,Accuracy:60.9601%,Area under PR:74.0522%,Area under ROC:60.5138%
      */
    println(f"${nbModelCats.getClass.getSimpleName},Accuracy:${nbAccuracyCats * 100}%2.4f%%,Area under PR:${nbPrCats * 100.0}%2.4f%%,Area under ROC:${nbRocCats * 100.0}%2.4f%%")


    //模型参数调优
    def trainWithParams(input:RDD[LabeledPoint],regParam:Double,numIterations:Int,updater:Updater,stepSize:Double) = {
      val lr = new LogisticRegressionWithSGD()
      lr.optimizer.setNumIterations(numIterations).setUpdater(updater).setRegParam(regParam).setStepSize(stepSize)
      lr.run(input)
    }

    //根据输入数据和相关模型，计算AUC
    def createMetrics(label:String,data:RDD[LabeledPoint],model:ClassificationModel) = {
      val scoreAndLabels = data.map(point => (model.predict(point.features),point.label))
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (label,metrics.areaUnderPR(),metrics.areaUnderROC())
    }
    //设置不同的迭代次数 一旦模型收敛，再多的迭代次数对模型影响不大
    val iterResults = Seq(1,5,10,50).map{
      param =>
        val model = trainWithParams(scaledDataCats,0.0,param,new SimpleUpdater,1.0)
        createMetrics(s"$param iterations",scaledDataCats,model)
    }

    /**
      * output:
      * 1 iterations,PR =74.59%, AUC = 64.95%
      * 5 iterations,PR =75.80%, AUC = 66.62%
      * 10 iterations,PR =75.80%, AUC = 66.55%
      * 50 iterations,PR =76.09%, AUC = 66.81%
      *
      */
    iterResults.foreach{case (label,pr,auc) => println(f"$label,PR =${pr * 100}%2.2f%%, AUC = ${auc * 100}%2.2f%%")}

    // 步长  步长太大容易震荡 即越过最优解
    val stepResults = Seq(0.001,0.01,0.1,1.0,10.0).map {
      param =>
        val model = trainWithParams(scaledDataCats,0.0,numIter,new SimpleUpdater,param)
        createMetrics(s"$param step size",scaledDataCats,model)
    }

    /**
      * output(可以看出步长过大对模型有负面影响):
      * 0.001 step size,PR = 74.60%,AUC = 64.97%
      * 0.01 step size,PR = 74.60%,AUC = 64.96%
      * 0.1 step size,PR = 75.00%,AUC = 65.52%
      * 1.0 step size,PR = 75.80%,AUC = 66.55%
      * 10.0 step size,PR = 72.57%,AUC = 61.92%
      */
    stepResults.foreach{case (param,pr,auc) => println(f"$param,PR = ${pr * 100}%2.2f%%,AUC = ${auc * 100}%2.2f%%")}

    /**
      * 正则化
      * MLlib中有三个正则化选项：
      * 1、SimpleUpdater:相当于没有正则化 此项是逻辑回归的默认配置
      * 2、SquaredL2Updater（ L2范数是指向量各元素的平方和然后求平方根，Ridge Regression，与L1不同的是，权值是接近于0而不是等于0）:L2正则化，是SVM的默认配置
      * 3、L1Updater（L1范数是指向量中各个元素绝对值之和，也有个美称叫“稀疏规则算子”（Lasso regularization））：L1正则化，会得到一个稀疏的权重向量，不重要的权重的值等于0
      *
      * 0.001 L2 regularizationo parameter,PR = 75.80%,AUC = 66.55%
      * 0.01 L2 regularizationo parameter,PR = 75.80%,AUC = 66.55%
      * 0.1 L2 regularizationo parameter,PR = 75.84%,AUC = 66.63%
      * 1.0 L2 regularizationo parameter,PR = 75.37%,AUC = 66.04%
      * 10.0 L2 regularizationo parameter,PR = 52.48%,AUC = 35.33%
      */
    val regResults = Seq(0.001,0.01,0.1,1.0,10.0).map{
      param =>
        val model = trainWithParams(scaledDataCats,param,numIter,new SquaredL2Updater,1.0)
        createMetrics(s"$param L2 regularizationo parameter",scaledDataCats,model)
    }
    regResults.foreach{case (param,pr,auc) => println(f"$param,PR = ${pr * 100}%2.2f%%,AUC = ${auc * 100}%2.2f%%")}

    /**
      * decision tree
      *
      * @param input 训练数据
      * @param maxDepth 深度
      * @param impurity 不纯度度量方式（Gini,Entropy）
      * @return
      *
      */
    def trainDTWithParams(input:RDD[LabeledPoint],maxDepth:Int,impurity: Impurity) = {
      DecisionTree.train(input,Algo.Classification,impurity,maxDepth)
    }

    /**
      *  Entropy熵
      *  1 tree depth,AUC = 59.33%
      *  2 tree depth,AUC = 61.68%
      *  3 tree depth,AUC = 62.61%
      *  4 tree depth,AUC = 63.63%
      *  5 tree depth,AUC = 64.88%
      *  10 tree depth,AUC = 76.26%
      *  20 tree depth,AUC = 98.45%
      */
    val dtResultsEntropy = Seq(1,2,3,4,5,10,20).map {
      param =>
        val model = trainDTWithParams(data,param,Entropy)
        val scoreAndLabels = data.map {
          point =>
            val score = model.predict(point.features)
            (if(score > 0.5) 1.0 else 0.0,point.label)
        }
        val metrics = new BinaryClassificationMetrics(scoreAndLabels)
        (s"$param tree depth",metrics.areaUnderROC())
    }

    dtResultsEntropy.foreach{case (param,auc) => println(f"$param,AUC = ${auc * 100}%2.2f%%")}

    /**
      * Gini指数
      * 1 tree depth,AUC = 59.33%
      * 2 tree depth,AUC = 61.68%
      * 3 tree depth,AUC = 62.61%
      * 4 tree depth,AUC = 63.63%
      * 5 tree depth,AUC = 64.89%
      * 10 tree depth,AUC = 78.37%
      * 20 tree depth,AUC = 98.87%
      */
    val dtResultsGini = Seq(1,2,3,4,5,10,20).map {
      param =>
        val model = trainDTWithParams(data,param, Gini)
        val scoreAndLabels = data.map {
          point =>
            val score = model.predict(point.features)
            (if(score > 0.5) 1.0 else 0.0,point.label)
        }
        val metrics = new BinaryClassificationMetrics(scoreAndLabels)
        (s"$param tree depth",metrics.areaUnderROC())
    }

    dtResultsGini.foreach{case (param,auc) => println(f"$param,AUC = ${auc * 100}%2.2f%%")}

    /**
      * native bayes
      */
    def trainNBWithParams(input:RDD[LabeledPoint],lambda:Double) = {
      val nb = new NaiveBayes()
      nb.setLambda(lambda)
      nb.run(input)
    }
    val nbResults = Seq(0.001,0.01,0.1,1.0,10.0).map {
      param =>
        val model = trainNBWithParams(dataNB,param)
        val scoreAndLabels = dataNB.map {
          point =>
            (model.predict(point.features),point.label)
        }
        val metrics = new BinaryClassificationMetrics(scoreAndLabels)
        (s"${param} lambda",metrics.areaUnderROC())
    }
    nbResults.foreach{case (param,auc) => println(f"$param,AUC = ${auc * 100}%2.2f%%")}

    /**
      * across validation
      *
      */
    val trainTestSplit = scaledDataCats.randomSplit(Array(0.6,0.4),123)
    val train = trainTestSplit(0)
    val test = trainTestSplit(1)

    val regResultsTest = Seq(0.0,0.001,0.0025,0.005,0.01).map{
      param =>
        val model = trainWithParams(train,param,numIter,new SquaredL2Updater,1.0)
        createMetrics(s"$param L2 regularization parameter",test,model)
    }
    regResultsTest.foreach{case (param,pr,auc) => println(f"$param,PR =  ${pr * 100}%2.6f%%,AUC = ${auc * 100}%2.6f%%")}
  }

}
