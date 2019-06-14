package com.yiban.spark.ml.dev;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.*;
import scala.Tuple2;

import java.util.Arrays;


/**
 * Created by 10000347 on 2016/3/1.
 */
public class LinerRegresion {
    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf().setAppName("Regression").setMaster(args[0]);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> data = sc.textFile(args[1]);
        JavaRDD<LabeledPoint> parsedData = data.map(line -> {
            String[] parts = line.split(",");
            double[] ds = Arrays.stream(parts[1].split(" "))
                    .mapToDouble(Double::parseDouble)
                    .toArray();
            return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(ds));
        }).cache();

        int numIterations = Integer.parseInt(args[2]); //迭代次数
        //无正则化
        LinearRegressionModel model = LinearRegressionWithSGD.train(parsedData.rdd(), numIterations);
        //L2正则化，又叫岭回归
        RidgeRegressionModel model1 = RidgeRegressionWithSGD.train(parsedData.rdd(), numIterations);
        //L1正则化
        LassoModel model2 = LassoWithSGD.train(parsedData.rdd(), numIterations);

        print(parsedData, model);
        print(parsedData, model1);
        print(parsedData, model2);
        //预测一条新数据方法
        double[] d = new double[]{1.0, 1.0, 2.0, 1.0, 3.0, -1.0, 1.0, -2.0};
        Vector v = Vectors.dense(d);
        System.out.println(model.predict(v));
        System.out.println(model1.predict(v));
        System.out.println(model2.predict(v));
    }

    public static void print(JavaRDD<LabeledPoint> parsedData, GeneralizedLinearModel model) {
        JavaPairRDD<Double, Double> valuesAndPreds = parsedData.mapToPair(
                point -> {
            //用模型预测训练数据
            double prediction = model.predict(point.features());
            return new Tuple2<>(point.label(), prediction);
        });
        //计算预测值与实际值差值的平方值的均值
        Double MSE = valuesAndPreds.mapToDouble((Tuple2<Double, Double> t) -> Math.pow(t._1() - t._2(), 2)).mean();
        System.out.println(model.getClass().getName() + " training Mean Squared Error = " + MSE);
    }
}
