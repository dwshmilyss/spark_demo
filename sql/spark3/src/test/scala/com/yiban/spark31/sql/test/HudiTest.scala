package com.yiban.spark31.sql.test

class HudiTest {

  var spark: SparkSession = _

  case class TestVO(id: Int, ts: Int, name: String, date: String)

  @BeforeEach
  def init() = {
    val logger: Logger = Logger.getLogger("org.apache.spark")
    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = SparkSession.builder()
      .appName("HudiTest")
      .master("local[2]")
      //        .config("spark.sql.warehouse.dir", s"hdfs://hdp1-test.leadswarp.com:8020/apps/hive/warehouse")
      //    .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .enableHiveSupport()
      .getOrCreate()
  }

  @Test
  def testCreateHudiTable(): Unit = {
    spark.sql("create table if not exists hudi_test_partition (id int,ts int,name string,date string ) using hudi options (type = 'cow',primaryKey = 'id',preCombineField = 'ts',hoodie.sql.origin.keygen.class='org.apache.hudi.keygen.SimpleKeyGenerator') partitioned by (date)")
    spark.sql("show tables").show()
  }

  @Test
  def testInsertHudiTable(): Unit = {
    spark.sql("insert into hudi_test_partition values (1,1,'aa','2015-01-01'),(2,1,'bb','2015-01-01'),(3,2,'cc','2015-01-02'),(4,2,'dd','2015-01-02')")
    //    spark.sql("insert into hudi_table partition(date='2015-01-01') select 1,1,'aa';")
    spark.sql("select * from hudi_test_partition").show()
  }

  @Test
  def testInsertHudiWithAPI(): Unit = {
    val df = spark.createDataFrame(Seq(TestVO(1, 1, "aa", "2015-01-01"), TestVO(2, 1, "bb", "2015-01-01"), TestVO(3, 2, "cc", "2015-01-02"), TestVO(4, 2, "dd", "2015-01-02")))
    df.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "id").
      option(PARTITIONPATH_FIELD_OPT_KEY, "date").
      //      option(INSERT_DROP_DUPS_OPT_KEY,false).
      option(TABLE_NAME, "hudi_test_partition").
      option(PAYLOAD_CLASS_OPT_KEY, classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName).
      option(OPERATION_OPT_KEY, "upsert").
//      option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true").
      //      option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://192.168.26.21:10000").
      //      option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://172.17.0.5:10000").
//      option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://localhost:10000").
//      option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY, "true").
//      option("hoodie.datasource.hive_sync.table", "hudi_test_partition").
//      option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "date").
      //      option("hoodie.clean.async", "true").
      //      option("hoodie.cleaner.commits.retained", "1").
      mode(Append).
      save("hdfs://localhost:9000/user/hive/warehouse/hudi_test_partition")

    spark.sql("select * from hudi_test_partition").show()
  }

  @Test
  def testPushDownGroupBy(): Unit = {
    val df = spark.sql("select count(1) from hudi_test_partition where ts>1 group by date")
    println(df.queryExecution)
    df.show()
  }

  @AfterEach
  def stop() = {
    spark.stop()
  }
}
