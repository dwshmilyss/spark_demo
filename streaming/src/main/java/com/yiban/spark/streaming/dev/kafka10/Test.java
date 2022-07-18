package com.yiban.spark.streaming.dev.kafka10;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @auther WEI.DUAN
 * @date 2019/3/28
 * @website http://blog.csdn.net/dwshmilyss
 */
public class Test {
    public static void main(String[] args) {
        Properties properties = new Properties();
        String path = Thread.currentThread().getContextClassLoader().getResource("test.properties").getPath();
        try {
            properties.load(new FileInputStream(path));
            System.out.println(properties.getProperty("aaa"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}