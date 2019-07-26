package com.yiban.spark.sql.javaudf;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
/**
 * @auther WEI.DUAN
 * @date 2018/8/10
 * @website http://blog.csdn.net/dwshmilyss
 */
public class JavaUDFExample {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "G:\\soft\\hadoop-2.8.2\\hadoop-2.8.2");
        SparkConf conf = new SparkConf().setAppName("java UDF example").setMaster("local");
        SparkSession spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate();

        String path = "file:///D:/source_code/sparkDemo/sql/src/main/resources/data/temperatures.json";
        Dataset<Row> ds = spark.read().json(path);
        ds.createOrReplaceTempView("citytemps");

        // Register the UDF with our SparkSession
        // UDF1<type1,type2> 第一个类型是传入参数类型，第二个类型是返回值类型
        spark.udf().register("CTOF", new UDF1<Double, Double>() {
            @Override
            public Double call(Double degreesCelcius) {
                return ((degreesCelcius * 9.0 / 5.0) + 32.0);
            }
        }, DataTypes.DoubleType);

        Dataset res = spark.sql("SELECT city, CTOF(avgLow) AS avgLowF, CTOF(avgHigh) AS avgHighF FROM citytemps");
        res.show();
        System.out.println("===================");
        res.explain();
    }
}
