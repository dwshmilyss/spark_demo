package com.yiban.spark.sql.util

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * Created by 10000735 on 2016/6/12.
  */
class DateUtils(dateType: String = "yyyy-MM-dd") extends Serializable {
  val sdf = new SimpleDateFormat(dateType) //dateType为需要返回的日期类型
  /**
    *
    * @param str 长整形的日期
    * @return
    */
  def long2Datestr(str: String): String = {
    val time = str.toLong
    //val sdf = new SimpleDateFormat(dateType)//"yyyyMMdd HH:mm:ss"
    val date = sdf.format(new Date(time.longValue() * 1000L))
    date
  }

  /**
    * 根据日期类型返回当前日期
    *
    * @return
    */
  def getNowDate(): String = {
    //val df = new SimpleDateFormat(dateType)//设置日期格式"yyyy-MM-dd HH:mm:ss"
    sdf.format(new Date())
  }

  /**
    * 获取每个月的第一天
    *
    * @return
    */
  def getMonthFirstDay(): String = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.MONTH, 0)
    cal.set(Calendar.DAY_OF_MONTH, 1)
    val firstDay = sdf.format(cal.getTime)
    firstDay
  }

  /**
    * 获取当前时间戳
    *
    * @return
    */
  def getNowTime(): Timestamp = {
    val date = new Date()
    val time = date.getTime
    val nowTime = new Timestamp(time)
    nowTime
  }

  /**
    * 获取昨天长整形的时间
    *
    * @return
    */
  def getYesterday2Long: Long = {
    val date = new Date()
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(Calendar.DATE, -1) //把日期往后增加一天.整数往后推,负数往前移动
    val yesterdayLong = sdf.parse(sdf.format(calendar.getTime)).getTime / 1000
    yesterdayLong
  }

  /**
    * 把日期转换为长整形
    *
    * @param date
    * @return
    */
  def strDate2Long(date: String): Long = {
    val d = sdf.parse(date)
    val time = d.getTime / 1000
    time
  }

  /**
    * 获取上月第一天的长整形时间
    *
    * @return
    */
  def getLastMonth2Long: Long = {
    //val date=new Date()
    val calendar = Calendar.getInstance()
    calendar.set(Calendar.DAY_OF_MONTH, 1)
    calendar.add(Calendar.MONTH, -1)
    val lastMonthLong = sdf.parse(sdf.format(calendar.getTime)).getTime / 1000
    lastMonthLong
  }

  /**
    * 获取当月的第一天长整形时间
    *
    * @return
    */
  def getCurrentMonth2Long: Long = {
    //val date=new Date()
    val calendar = Calendar.getInstance()
    calendar.set(Calendar.DAY_OF_MONTH, 1)
    calendar.add(Calendar.MONTH, 0)
    val currentMonthLong = sdf.parse(sdf.format(calendar.getTime)).getTime / 1000
    currentMonthLong
  }

  /**
    * 根据给的日期，推出下月，并返回下月的长整形时间
    *
    * @param date
    * @return
    */
  def date2GetNextMonth(date: String): Long = {
    val calendar = Calendar.getInstance()
    val parseDate = sdf.parse(date) //转换为日期
    calendar.setTime(parseDate)
    calendar.add(Calendar.MONTH, 1) //当前日期的下月
    val nextMonth = sdf.format(calendar.getTime)
    val nextMonthLong = sdf.parse(nextMonth).getTime / 1000
    nextMonthLong
  }

  /**
    * 获取上一月份
    *
    * @return
    */
  def getLastMonth(): String = {
    val calendar = Calendar.getInstance()
    val date = new Date()
    calendar.setTime(date)
    calendar.add(Calendar.MONTH, -1)
    val lastMonth = sdf.format(calendar.getTime)
    lastMonth
  }

  /**
    * 获取当年年份
    * @return
    */
  def getCurrentYear(): Int = {
    val calendar = Calendar.getInstance()
    val year = calendar.get(Calendar.YEAR);
    return year;
  }

}

