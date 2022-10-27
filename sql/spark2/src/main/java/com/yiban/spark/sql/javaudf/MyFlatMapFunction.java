package com.yiban.spark.sql.javaudf;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @Description: TODO
 * @Author: David.duan
 * @Date: 2022/10/27
 **/
public class MyFlatMapFunction implements FlatMapFunction<String,String> {
    @Override
    public Iterator<String> call(String str) throws Exception {
        return Arrays.asList(str.split(" ")).iterator();
    }
}
