package com.yiban.spark.sql.dev

import java.sql.Timestamp
import java.util.{Calendar, Date}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.yiban.spark.sql.util.{CommonUtils, DbConn, HelpUsage, RegexUtils}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

object DoubleIndex {

  val logger: Logger = LoggerFactory.getLogger("org.apache.spark")

  case class ActiveUserInfo(userId: Int, product: String) extends java.io.Serializable

  case class UserInfoSchoolSexMobile(userId: Int, schoolId: Int, groupId: Int, classId: Int) extends java.io.Serializable

  def getCurrentYear(): Int = {
    val calendar = Calendar.getInstance()
    val year = calendar.get(Calendar.YEAR)
    return year
  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("DoubleIndex")

    /**
      * 启用kryo序列化
      */
    //    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //    conf.set("spark.kryo.registrationRequired", "true")
    /**
      * 这个直接配置即可，不需要额外编写注册类，如果需要强制注册，则加上 conf.set("spark.kryo.registrationRequired", "true")
      */
    //    conf.registerKryoClasses(Array(classOf[ActiveUserInfo], classOf[UserInfoSchoolSexMobile],Class.forName("org.apache.spark.sql.execution.columnar.CachedBatch")))
    //对RDD进行压缩以节省内存空间
    //    conf.set("spark.rdd.compress", "true")
    logger.warn("logger ccc")
    println("logger ccc")

    val sc = new SparkContext(conf)
//    val dateType = "m"
    val dateType = args(8)
    logger.warn("logger " + dateType)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

        val logTxt = sc.textFile(args(0)) //"hdfs://yiban.isilon:8020/logs/201607/template_pro*.log"
//    val logTxt = sc.textFile("hdfs://master01:9000/logs") //"hdfs://yiban.isilon:8020/logs/201607/template_pro*.log"
//    var dateStr = "2018-02"
    var dateStr = args(9)
    val commUtils = new CommonUtils
    val beginEnd = commUtils.calcuBeginAndEndTime(dateType, dateStr).split("@") //计算起始时间和截止时间
    val beginTime = beginEnd(0).toLong
    val endTime = beginEnd(1).toLong

    val regexUtils = new RegexUtils
    val fromRegex: String = "^\\{.*\\}$"

    //获取一个月的日志数据
    val logData = logTxt.map(line => {
      var data: JSONObject = null
      try {
        val tempLine = line.trim
        if (regexUtils.regexJSON(tempLine, fromRegex)) {
          val data = JSON.parseObject(tempLine)
          val product = data.getString("product")
          val time = data.getLong("time")
          //过滤掉不在规定时间范围内的数据
          if (!product.equals("") && time >= beginTime && time < endTime) {
            //获取资料库 易喵喵 易运动的数据
            if (product.equalsIgnoreCase("file") || product.equalsIgnoreCase("ymm") || product.equalsIgnoreCase("esport") || product.equalsIgnoreCase("blog") || product.equalsIgnoreCase("questionnaire") || product.equalsIgnoreCase("fastBuild") || product.equalsIgnoreCase("database") || product.equalsIgnoreCase("forum") || product.equalsIgnoreCase("vote")) {
              var uid = -1
              var temp = ""
              //获取掉src_obj非法的数据
              if (data.get("src_obj").isInstanceOf[JSONObject]) {
                temp = data.getJSONObject("src_obj").getString("uid")
                //过滤掉USERID为空的数据
                if (!temp.trim.isEmpty)
                  uid = temp.toInt
                //过滤掉userId非法的数据
                if (uid != 0 && uid != -1)
                  ActiveUserInfo(uid, product)
              }
            }
          }
        }
      } catch {
        case e: Exception => println("=======parse data exception:" + e + data)
      }
    }).filter(x => x.isInstanceOf[ActiveUserInfo]).map(x => x.asInstanceOf[ActiveUserInfo])
    //    logData.cache()
    logData.persist(StorageLevel.MEMORY_ONLY_SER)

    //读取用户信息数据"/spark/data/user_pro.txt"
    val userDataTxt = sc.textFile(args(6))
    //    val userDataTxt = sc.textFile("hdfs://master01:9000/data/userinfo/user_pro.txt")
    //从用户信息中获取需要的数据为存储到UserInfoJoinSchoolMobile中
    val userData = userDataTxt.map { line =>
      val u = line.split(" ")
      try {
        //获取用户入学年份不为空的数据
        if (!u(8).trim.isEmpty) {
          val joinSchoolYear = u(8).toInt
          //获取入学年份不超过三年的学生
          if (getCurrentYear - joinSchoolYear <= 3) {
            //过滤掉没有校方认证的用户
            if (!u(14).isEmpty && u(14).toInt != 0) {
              //过滤掉学校 学院 班级信息都没有的学生
              if (!"-1".equals(u(6)) || !"-1".equals(u(19)) || !"-1".equals(u(22))) {
                UserInfoSchoolSexMobile(u(0).toInt, u(6).toInt, u(19).toInt, u(22).toInt)
              }
            }
          }
        }
      } catch {
        case e: Exception => println("NumberFormatException:" + e)
      }
    }
    val allUserData = userData.filter(x => x.isInstanceOf[UserInfoSchoolSexMobile]).map{
      x =>
        val b = x.asInstanceOf[UserInfoSchoolSexMobile]
      logger.warn("userinfo : userId = " + b.userId +",schoolId = "+ b.schoolId + ",groupId = "+b.groupId + ",classId = " + b.classId)
      println("userinfo : userId = " + b.userId +",schoolId = "+ b.schoolId + ",groupId = "+b.groupId + ",classId = " + b.classId)
      System.out.println("userinfo : userId = " + b.userId +",schoolId = "+ b.schoolId + ",groupId = "+b.groupId + ",classId = " + b.classId)
        b
    }
    //    allUserData.cache()
    allUserData.persist(StorageLevel.MEMORY_ONLY_SER)

    logData.toDF().createOrReplaceTempView("ActiveUserInfo")
    allUserData.toDF().createOrReplaceTempView("UserInfoSchoolSexMobile")

    val tempLong = new java.lang.Long(beginTime)
    /**
      * 这里转换的时候一定要乘以1000，不然转换后的时间永远是1970年mr
      */
    val date: java.util.Date = new Date(tempLong * 1000)
    val javaSqlTime: java.sql.Timestamp = new Timestamp(tempLong * 1000)

    //统计UV
    val schoolUV = sqlContext.sql("select count(distinct a.userId) as cn,product,schoolId as catagory_id,0 as catagory,0 as isPV,'" + javaSqlTime + "' as insertTime from ActiveUserInfo a join UserInfoSchoolSexMobile u on a.userId=u.userId where schoolId is not null group by product,schoolId")
    val groupUV = sqlContext.sql("select count(distinct a.userId) as cn,product,groupId as catagory_id,1 as catagory,0 as isPV,'" + javaSqlTime + "' as insertTime from ActiveUserInfo a join UserInfoSchoolSexMobile u on a.userId=u.userId where groupId is not null group by product,groupId")
    val classUV = sqlContext.sql("select count(distinct a.userId) as cn,product,classId as catagory_id,2 as catagory,0 as isPV,'" + javaSqlTime + "' as insertTime from ActiveUserInfo a join UserInfoSchoolSexMobile u on a.userId=u.userId where classId is not null group by product,classId")
    //统计PV
    val schoolPV = sqlContext.sql("select count(1) as cn,product,schoolId as catagory_id,0  as catagory,1 as isPV,'" + javaSqlTime + "' as insertTime from ActiveUserInfo a join UserInfoSchoolSexMobile u on a.userId=u.userId where schoolId is not null group by product,schoolId")
    val groupPV = sqlContext.sql("select count(1) as cn,product,groupId as catagory_id,1 as catagory,1 as isPV,'" + javaSqlTime + "' as insertTime from ActiveUserInfo a join UserInfoSchoolSexMobile u on a.userId=u.userId where groupId is not null group by product,groupId")
    val classPV = sqlContext.sql("select count(1) as cn,product,classId as catagory_id,2 as catagory,1 as isPV,'" + javaSqlTime + "' as insertTime from ActiveUserInfo a join UserInfoSchoolSexMobile u on a.userId=u.userId where classId is not null group by product,classId")
    //统计总的UV
    val result = schoolUV.union(groupUV).union(classUV).union(schoolPV).union(groupPV).union(classPV)
    //合并PV和UV
    //    println(result.collect().length)
    result.cache()
    //把数据写入MySQL
    val dbConn = new DbConn
    dbConn.writeData2Mysql1(result, args(1), args(2), args(3), args(4), args(5), "DoubleIndex") //ip,端口,数据库名,用户名,密码,表名
    //    dbConn.writeData2Mysql1(result,"10.21.3.120", "3306", "yiban_BI", "root", "wenha0", "DoubleIndex") //ip,端口,数据库名,用户名,密码,表名

    result.unpersist()
    logData.unpersist()
    sc.stop()
  }
}
