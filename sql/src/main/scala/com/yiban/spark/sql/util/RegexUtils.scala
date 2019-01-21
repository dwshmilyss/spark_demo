package com.yiban.spark.sql.util

import java.util.regex.Pattern

/**
 * Created by 10000735 on 2016/5/12.
 * 过滤HTML标签
 */
class RegexUtils extends Serializable{
  //val fromRegex="<([^>]*)>"  // 过滤所有以<开头以>结尾的html标签
  def regexUtils( str:String,fromRegex:String)={
    val pattern = Pattern.compile(fromRegex)
    val matcher = pattern.matcher(str)
    val sb = new StringBuffer()
    var result1 = matcher.find()
    while (result1) {
      matcher.appendReplacement(sb, "")
      result1 = matcher.find()
    }
    matcher.appendTail(sb)
    sb.toString()
  }


  def regexJSON(str:String,fromRegex:String) : Boolean = {
    val pattern = Pattern.compile(fromRegex)
    val matcher = pattern.matcher(str)
    if (matcher.find()){
      return true
    }else{
      return false
    }
  }
}

