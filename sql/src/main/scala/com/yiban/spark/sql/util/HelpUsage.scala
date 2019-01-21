package com.yiban.spark.sql.util

/**
 * Created by 10000735 on 2016/7/28.
 */
class HelpUsage extends Serializable{
  /**
   * 校验日期类型是否合法
   * @param dateType
   */
  def checkDateType(dateType:String):Unit={
    if(!dateType.equals("d") && !dateType.equals("m")){
      System.err.println("第七个参数请输入m或d，表示执行一月还是一天数据"+dateType)
      System.exit(1)
    }
  }
  /**
   * 7个参数，第7个可选
   */
  def usage7(): Unit ={
    System.err.println("参数说明，参数之间用空格分隔，需要输入七个参数：\n第一个参数是日志文件路径【例如：hdfs://mycluster1/spark/data/app_feeds_json_201602-201605.txt】 " +
      "\n第二个参数是数据库IP地址【例如：10.21.3.73】 \n第三个参数是数据库端口【例如：3306】 \n第四个参数是数据库名【例如：yiban_BI】 " +
      "\n第五个参数是用户名【例如root】 \n第六个参数是数据库密码【例如：wenh0】\n第七个参数是执行哪一天数据(可选，默认前一天)【例如：2016-07-28】")
    System.exit(1)
  }
  /**
   * 8个参数，第8个可选
   */
  def usage8(): Unit ={
    System.err.println("参数说明，参数之间用空格分隔，需要输入七个参数：\n第一个参数是日志文件路径【例如：hdfs://mycluster1/spark/data/app_feeds_json_201602-201605.txt】 " +
      "\n第二个参数是数据库IP地址【例如：10.21.3.73】 \n第三个参数是数据库端口【例如：3306】 \n第四个参数是数据库名【例如：yiban_BI】 " +
      "\n第五个参数是用户名【例如root】 \n第六个参数是数据库密码【例如：wenh0】\n第七个参数是按日或按月【m/d 选一】 \n第八个参数是执行哪一天或哪一月数据(可选，默认前一天或前一月)【例如：2016-07-28/2016-07】")
    System.exit(1)
  }
  /**
   * 9个参数，第9个可选
   */
  def usage9(): Unit ={
    System.err.println("参数说明，参数之间用空格分隔，需要输入七个参数：\n第一个参数是日志文件路径【例如：hdfs://mycluster1/spark/data/app_feeds_json_201602-201605.txt】 " +
      "\n第二个参数是数据库IP地址【例如：10.21.3.73】 \n第三个参数是数据库端口【例如：3306】 \n第四个参数是数据库名【例如：yiban_BI】 " +
      "\n第五个参数是用户名【例如root】 \n第六个参数是数据库密码【例如：wenh0】\n第七个参数是用户数据【例如：hdfs://yiban.isilon:8020/data/user_pro.txt】\n第八个参数是用户数据【例如：hdfs://yiban.isilon:8020/data/uid0.txt】 " +
      "\n第九个参数是执行哪一天或哪一月数据(可选，默认前一天或前一月)【例如：2016-07-28/2016-07】")
    System.exit(1)
  }
  /**
   * 10个参数，第10个可选
   */
  def usage10(): Unit ={
    System.err.println("参数说明，参数之间用空格分隔，需要输入八个参数：\n第一个参数是日志文件路径【例如：hdfs://mycluster1/spark/data/app_feeds_json_201602-201605.txt】 " +
      "\n第二个参数是数据库IP地址【例如：10.21.3.73】 \n第三个参数是数据库端口【例如：3306】 \n第四个参数是数据库名【例如：yiban_BI】 " +
      "\n第五个参数是用户名【例如root】 \n第六个参数是数据库密码【例如：wenh0】\n第七个参数是用户数据【例如：hdfs://yiban.isilon:8020/data/user_pro.txt】\n第八个参数是用户数据【例如：hdfs://yiban.isilon:8020/data/uid0.txt】 " +
      "\n第九个参数是按日或按月【m/d 选一】\n第十个参数是执行哪一天或哪一月数据(可选，默认前一天或前一月)【例如：2016-07-28/2016-07】")
    System.exit(1)
  }
  /**
   * 10个参数，第10个可选
   */
  def usage10PublicApp(): Unit ={
    System.err.println("参数说明，参数之间用空格分隔，需要输入八个参数：\n第一个参数是日志文件路径【例如：hdfs://mycluster1/spark/data/app_feeds_json_201602-201605.txt】 " +
      "\n第二个参数是数据库IP地址【例如：10.21.3.73】 \n第三个参数是数据库端口【例如：3306】 \n第四个参数是数据库名【例如：yiban_BI】 " +
      "\n第五个参数是用户名【例如root】 \n第六个参数是数据库密码【例如：wenh0】\n第七个参数是用户数据【例如：hdfs://yiban.isilon:8020/data/user_pro.txt】\n第八个参数是用户数据【例如：hdfs://yiban.isilon:8020/data/uid0.txt】 " +
      "\n第九个参数是公共群对应的总人数 【hdfs://mycluster1/GroupPublicMember.txt】\n第十个参数是执行哪一天或哪一月数据(可选，默认前一天或前一月)【例如：2016-07-28/2016-07】")
    System.exit(1)
  }

  /**
   * 参数长度错误
   * @param paramLenth
   */
  def paramLengthErr(paramLenth:Int):Unit={
    System.err.println("参数个数不正确，参数个数应小于等于:"+paramLenth)
    System.exit(1)
  }
}
