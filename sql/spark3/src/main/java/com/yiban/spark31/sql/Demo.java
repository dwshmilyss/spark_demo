package com.yiban.spark31.sql;

/**
 * @Description: TODO
 * @Author: David.duan
 * @Date: 2022/7/15
 **/
public class Demo {
    public static void main(String[] args) {
        System.out.println("path = " + Demo.class.getClassLoader().getResource("data/person.json").getPath());
    }
}
