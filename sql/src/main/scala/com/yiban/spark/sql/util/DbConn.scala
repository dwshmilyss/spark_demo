package com.yiban.spark.sql.util

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
 * Created by 10000735 on 2016/6/23.
 */
class DbConn extends Serializable{
  def getConnection(databaseIP:String,port:String,databaseName:String,username:String,password:String):Connection = {
    var conn: Connection = null
    try{
      conn = DriverManager.getConnection("jdbc:mysql://"+databaseIP+":"+port+"/"+databaseName+"?characterEncoding=utf-8", username, password)
    }catch {
      case e:Exception => println("======connection mysql excption ,exception info is:"+e)
    }
    conn
  }

  def colseConn(conn: Connection,ps: PreparedStatement): Unit ={
    try{
      if(ps != null){
        ps.close()
      }
      if(conn != null){
        conn.close()
      }
    }catch {
      case e:Exception => println("======colse mysql connection exception:"+e)
    }
  }

  /**
   * 从MySQL读取自定义词
   * @param sqlContext
   * @param allDefineWords 是否找出所有的自定义词
   * @param firstDay 每个月的第一天
   * @param connectUrl
   * @param username
   * @param password
   * @return java.util.List[Row]
   */
  def readDefineWordsFromMysql(sqlContext:SQLContext,allDefineWords:Boolean,defineOrStopWords:DefineOrStopWords,firstDay:String,connectUrl:String,username:String,password:String): DataFrame ={
    //val connectUrl="jdbc:mysql://"+databaseIP+":"+port+"/"+databaseName+"?characterEncoding=utf-8"
    var words:DataFrame=null
    try{
      val properties =new Properties()
      properties.put("user", username)
      properties.put("password", password)
      val jdbcDF = sqlContext.read.jdbc(connectUrl, "SegUserDefineWords", properties)
      if(defineOrStopWords == DefineOrStopWords.DefineWords){
        if(allDefineWords){
          words=jdbcDF.select("word","type")  //找出所有自定义词
        }else{
          words=jdbcDF.select("word","type","createTime").where("createTime>='"+firstDay+"'")  //找出自定义词大于当前月1号的数据,在出现指定新词时使用
        }
      }else if(defineOrStopWords == DefineOrStopWords.StopWords){
        words=jdbcDF.select("word","type")  //找出所有停用词.where("type=1")
      }
    }catch {
      case e:Exception => println("======read words from mysql exception:"+e)
    }
    words
    //words.collectAsList()
  }

  /**
   * 把DataFrame数据结果写入到MySQL
   * @param writeData
   * @param partitionBy  分区参数
   * @param databaseIP
   * @param port
   * @param databaseName
   * @param username
   * @param password
   * @param tableName
   */
  def writeData2Mysql(writeData:DataFrame,partitionBy:String,databaseIP:String,port:String,databaseName:String,username:String,password:String,tableName:String):Unit={
    try{
      val connectURL="jdbc:mysql://"+databaseIP+":"+port+"/"+databaseName+"?characterEncoding=utf-8"
      val properties=new Properties()
      properties.put("user", username)
      properties.put("password", password)
      writeData.write.mode(SaveMode.Append).partitionBy(partitionBy).jdbc(connectURL,tableName,properties)

//      writeData.write
//        .format("jdbc")
//          .mode(SaveMode.Append)
//            .partitionBy("")
//        .option("url", "jdbc:postgresql:dbserver")
//        .option("dbtable", "schema.tablename")
//        .option("user", "username")
//        .option("password", "password")
//
//        .save()
    }catch {
      case e:Exception => println("===========write data to mysql exception:"+e)
    }
  }


  def writeData2Mysql1(writeData:DataFrame,databaseIP:String,port:String,databaseName:String,username:String,password:String,tableName:String):Unit={
    try{
      val connectURL="jdbc:mysql://"+databaseIP+":"+port+"/"+databaseName+"?characterEncoding=utf-8"
      val properties=new Properties()
      properties.put("user", username)
      properties.put("password", password)
      writeData.write.mode(SaveMode.Append).jdbc(connectURL,tableName,properties)

      //      writeData.write
      //        .format("jdbc")
      //          .mode(SaveMode.Append)
      //            .partitionBy("")
      //        .option("url", "jdbc:postgresql:dbserver")
      //        .option("dbtable", "schema.tablename")
      //        .option("user", "username")
      //        .option("password", "password")
      //
      //        .save()
    }catch {
      case e:Exception => println("===========write data to mysql exception:"+e)
    }
  }
}
