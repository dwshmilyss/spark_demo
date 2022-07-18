package com.yiban.spark.sql.util

/**
 * Created by 10000735 on 2016/7/29.
 */
class CommonUtils extends Serializable{
  /**
   * 根据日期类型和日期返回起始时间和截止时间
   * @param dateType
   * @param date
   * @return
   */
    def calcuBeginAndEndTime(dateType:String,date:String):String={
      val dateUtils=new DateUtils()
      val monthUtils=new DateUtils("yyyy-MM")
      var beginTime:Long=0
      var endTime:Long=0
      if(dateType.equals("m")){//执行一月的数据
        if(!date.equals("")){//指定了执行某月的数据
          beginTime=monthUtils.strDate2Long(date)
          endTime=monthUtils.date2GetNextMonth(date)
        }else{//默认执行上月的数据
          beginTime=monthUtils.getLastMonth2Long
          endTime=monthUtils.getCurrentMonth2Long
        }
      }else if(dateType.equals("d")){//执行一天的数据
        if(!date.equals("")){//执行某天的数据
          beginTime=dateUtils.strDate2Long(date)
        }else{//默认执行前一天的数据
          beginTime=dateUtils.getYesterday2Long //获取前一天的长整形时间
        }
        endTime=beginTime+86400
      }
      val beginEnd=beginTime+"@"+endTime
      beginEnd
    }



  //校验数据
  def verifyData(str:String):String={
    try{
      if(str.indexOf("gid") != -1 && str.indexOf("guid") != -1 ){
        return "1"
      }else{
        return "0"
      }
    }catch {
      case e:Exception => println("verifyForumData exception:"+e) ;return "0"
    }
  }

  /**
   * 截取URL
   * @param str
   * @return
   */
  def subUrl(str:String):String={
    if(str.equals("") || str == null){
      return ""
    }
    try{
      var start=0
      if(str.indexOf("//") != -1){
        start=str.indexOf("//") + 2
      }
      var end=str.length
      if(str.indexOf("?") != -1){
        end=str.indexOf("?")
      }
      var res =  str.substring(start, end)
      if (res.length() > 1000) {
        res = res.substring(0,1000)
      }
      res
    }catch {
      case e:Exception => println("subUrl exception:"+e) ;return ""
    }
  }
}
