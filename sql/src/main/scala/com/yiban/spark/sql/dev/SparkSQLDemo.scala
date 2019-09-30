package com.yiban.spark.sql.dev

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object SparkSQLDemo {

  val logger: Logger = Logger.getLogger("org.apache.spark")
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("SparkSQLDemo")
    .master("local[*]")
    .config("fs.defaultFS", "file:///")
    .getOrCreate()


  import spark.implicits._

  val people_json_path: String = SparkSQLDemo.getClass.getClassLoader.getResource("data/people.json").getPath

  val people_txt_path: String = SparkSQLDemo.getClass.getClassLoader.getResource("data/people.txt").getPath

  val employee_json_path = SparkSQLDemo.getClass.getClassLoader.getResource("data/employees.json").getPath

  def main(args: Array[String]): Unit = {
    test4()
  }

  def test() = {

    val df = spark.read.json(people_json_path)
    df.show()
    df.printSchema()
    df.select("name").show()
    df.select($"name", $"age" + 1).show()
    df.filter($"age" > 21).show()
    //这里只能筛选原始数据 加工后的数据用filter是无效的
    df.select($"name", $"age" + 1).filter($"age" >= 19).show()
    df.groupBy("age").count().show()
    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("select * from people")
    sqlDF.show()
    println("====== GlobalTempView 可以在不同的session中使用 ======")
    df.createGlobalTempView("people")
    spark.sql("SELECT * FROM global_temp.people").show()
    spark.newSession().sql("SELECT * FROM global_temp.people").show()

  }

  //这里的Person不能放在test1()里面
  case class Person(name: String, age: Long)

  def test1() = {
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect().foreach(println)
    val peopleDS = spark.read.json(people_json_path).as[Person]
    println("============== dataset ================ ")
    peopleDS.printSchema()
    //dataset转rdd[Person]
    peopleDS.rdd
    println("============== dataframe ================ ")
    val peopleDF = peopleDS.toDF()
    peopleDF.printSchema()
    //dataframe转rdd[Row]
    peopleDF.rdd
    println("============== 交换属性位置 ================ ")
    peopleDS.map(person => Person(person.name, person.age)).printSchema()

    peopleDS.createOrReplaceTempView("people")
    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")
    val teenagersDS = teenagersDF.as[Person]
    println("================== teenagersDS ===============")
    teenagersDS.printSchema()
    teenagersDF.map(teenager => "age : " + teenager(1)).show()
    teenagersDF.map(teenager => "name: " + teenager.getAs[String]("name")).show()

    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
  }

  def test2() = {
    import org.apache.spark.sql.types._
    val peopleRDD = spark.sparkContext.textFile(people_txt_path)
    val schemaString = "name age"
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val rowRDD = peopleRDD.map(_.split(",")).map(row => Row(row(0), row(1).trim))
    val peopleDF = spark.createDataFrame(rowRDD, schema)
    peopleDF.createOrReplaceTempView("people")
    val res = spark.sql("select name from people")
    res.show()
    res.map(x => "Name = " + x(0)).show()
  }


  /**
    * RDD转DataFrame 示例1
    *
    * @return
    */
  def rddToDFByCaseClass(): DataFrame = {
    val peopleRDD = spark.sparkContext.textFile(people_txt_path)
      .map(_.split(",")).map(person => Person(person(0), person(1).trim.toLong))
    val peopleDF = peopleRDD.toDF()
    peopleDF
  }

  /**
    * RDD转DataFrame 示例2
    *
    * @return
    */
  def rddToDFBySchema(): DataFrame = {
    val schema = StructType(
      Seq(
        StructField("name", StringType, true),
        StructField("age", LongType, true)
      )
    )
    val peopleRDD = spark.sparkContext.textFile(people_txt_path)
      .map(_.split(",")).map(row => Row(row(0), row(1).trim.toLong))
    spark.createDataFrame(peopleRDD, schema)
  }

  /**
    * 自定义函数求均值
    * 要继承 UserDefinedAggregateFunction
    */
  object MyAverage extends UserDefinedAggregateFunction {
    // Data types of input arguments of this aggregate function
    override def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)

    // Data types of values in the aggregation buffer
    override def bufferSchema: StructType = {
      StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
    }

    // The data type of the returned value
    override def dataType: DataType = DoubleType

    //此函数是否始终在相同输入上返回相同的输出
    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }

    // Updates the given aggregation buffer `buffer` with new input data from `input`
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(1) + 1
      }
    }

    // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    // Calculates the final result
    override def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
  }

  def test3()={
    spark.udf.register("myAverage", MyAverage)
    val df = spark.read.json(employee_json_path)
    df.createOrReplaceTempView("employees")
    df.show()

    //下面2个都是可以求均值的
    val res = spark.sql("select myAverage(salary) as average_salary from employees")
    val res1 = spark.sql("select avg(salary) as average_salary from employees")

    res1.show()
  }

  /**
    * 类型安全的自定义函数
    */
  case class Employee(name: String,salary: Long)
  case class Average(var sum: Long,var count: Long)
  object MyAverageTypeSafe extends Aggregator[Employee,Average,Double]{
    // A zero value for this aggregation. Should satisfy the property that any b + zero = b
    override def zero: Average = Average(0L,0L)

    // Combine two values to produce a new value. For performance, the function may modify `buffer`
    // and return it instead of constructing a new object
    override def reduce(buffer: Average, employee: Employee): Average = {
      buffer.sum += employee.salary
      buffer.count += 1
      buffer
    }
    // Merge two intermediate values
    override def merge(b1: Average, b2: Average): Average = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }

    override def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

    override def bufferEncoder: Encoder[Average] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

  def test4() = {
    val ds = spark.read.json(employee_json_path).as[Employee]
    val averageSalary = MyAverageTypeSafe.toColumn.name("average_salary")
    val res = ds.select(averageSalary)
    res.show()
  }

}

