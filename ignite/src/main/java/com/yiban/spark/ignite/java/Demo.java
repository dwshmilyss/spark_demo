package com.yiban.spark.ignite.java;

import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Demo {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("ignite")
                .master("local")
                .config("spark.executor.instances", "2")
                .getOrCreate();
        String cfgPath = Demo.class.getClassLoader().getResource("local-config.xml").getPath();
        System.out.println("cfgPath = " + cfgPath);

        Dataset<Row> df = spark.read()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())               // Data source type.
                .option(IgniteDataFrameSettings.OPTION_TABLE(), "person")      // Table to read.
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), cfgPath) // Ignite config.
                .load();

        df.createOrReplaceTempView("person");

        Dataset<Row> igniteDF = spark.sql("select * from person where id=3");
        igniteDF.show();
    }

}
