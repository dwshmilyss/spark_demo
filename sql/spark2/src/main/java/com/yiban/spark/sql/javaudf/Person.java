package com.yiban.spark.sql.javaudf;

/**
 * @Description: TODO
 * @Author: David.duan
 * @Date: 2022/10/27
 **/
public class Person {
    String name;
    String sex;
    long age;
    long height;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public long getAge() {
        return age;
    }

    public void setAge(long age) {
        this.age = age;
    }

    public long getHeight() {
        return height;
    }

    public void setHeight(long height) {
        this.height = height;
    }
}
