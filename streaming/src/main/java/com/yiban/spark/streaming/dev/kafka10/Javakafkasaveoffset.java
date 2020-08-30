package com.yiban.spark.streaming.dev.kafka10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.collection.Iterator;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.util.*;

/**
 * 利用hbase 实现kafka的精准一次消费
 *
 * @auther WEI.DUAN
 * @date 2017/5/23
 * @website http://blog.csdn.net/dwshmilyss
 */
public class Javakafkasaveoffset {

    public static final Logger logger = LoggerFactory.getLogger("org.apache.spark");

    public static void main(String[] args) throws Exception {
        org.apache.log4j.Logger.getLogger("org.apache.kafka").setLevel(org.apache.log4j.Level.ERROR);
        org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(org.apache.log4j.Level.ERROR);
        if (args.length < 5) {
            System.err.println("Usage: JavaDirectKafkaOffsetSaveToHBase <kafka-brokers> <topics> <topic-groupid> <zklist> <datatable>\n\n");
            System.exit(1);
        }

        JavaInputDStream<ConsumerRecord<String, String>> stream = null;
        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.toLevel("WARN"));

        String brokers = args[0];  //Kafka brokers list， 用逗号分割
        String topics = args[1];  //要消费的话题，目前仅支持一个，想要扩展很简单，不过需要设计一下保存offset的表，请自行研究
        String groupid = args[2];  //指定消费者group
        String zklist = args[3];  //hbase连接要用到zookeeper server list，用逗号分割
        final String datatable = args[4];  //想要保存消息offset的hbase数据表

        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaOffsetSaveToHBase");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", groupid);
        kafkaParams.put("auto.offset.reset", "earliest"); //默认第一次执行应用程序时从Topic的首位置开始消费
        kafkaParams.put("enable.auto.commit", "false");   //不使用kafka自动提交模式，由应用程序自己接管offset

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));  //此处把1个或多个topic解析为集合对象，因为后面接口需要传入Collection类型

        //建立hbase连接
        final Configuration conf = HBaseConfiguration.create(); // 获得配置文件对象
        conf.set("hbase.zookeeper.quorum", zklist);
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        //获得连接对象
        Connection con = ConnectionFactory.createConnection(conf);
        Admin admin = con.getAdmin();

        //logger.warn(  " @@@@@@@@ " + admin ); //调试命令，判断连接是否成功
        TableName tableName = TableName.valueOf(datatable); //创建表名对象


        /*存放offset的表模型如下，请自行优化和扩展，请把每个rowkey对应的record的version设置为1（默认值），因为要覆盖原来保存的offset，而不是产生多个版本
         *----------------------------------------------------------------------------------------------
         *  rowkey           |  column family                                                          |
         *                   --------------------------------------------------------------------------
         *                   |  column:topic(string)  |  column:partition(int)  |   column:offset(long)|
         *----------------------------------------------------------------------------------------------
         * topic_partition   |   topic                |   partition             |    offset            |
         *----------------------------------------------------------------------------------------------
         */

        //判断数据表是否存在，如果不存在则从topic首位置消费，并新建该表；如果表存在，则从表中恢复话题对应分区的消息的offset
        boolean isExists = admin.tableExists(tableName);
        logger.warn("tale isExists = " + isExists);
        if (isExists) {
            try {
                Table table = con.getTable(tableName);

                Filter filter = new RowFilter(CompareOp.GREATER_OR_EQUAL,
                        new BinaryComparator(Bytes.toBytes(topics + "_")));
                Scan scan = new Scan();
                scan.setFilter(filter);
                ResultScanner rs = table.getScanner(scan);

                // begin from the the offsets committed to the database
                Map<TopicPartition, Long> fromOffsets = new HashMap<>();
                String topic = null;
                int partition = 0;
                long offset = 0;
                for (Result result : rs) {
                    //获取rowkey
                    logger.warn("rowkey:" + new String(result.getRow()));

                    //老的hbase获取数据写法
                    for (KeyValue keyValue : result.raw()) {
                        String columnName = new String(keyValue.getQualifier());
                        if ("topic".equals(columnName)) {
                            topic = Bytes.toString(keyValue.getValue());
                            logger.warn("列族:" + new String(keyValue.getFamily())
                                    + " 列:" + new String(keyValue.getQualifier()) + ":"
                                    + topic);
                        }

                        if ("partition".equals(columnName)) {
                            partition = Bytes.toInt(keyValue.getValue());
                            logger.warn("列族:" + new String(keyValue.getFamily())
                                    + " 列:" + new String(keyValue.getQualifier()) + ":"
                                    + partition);
                        }

                        if ("offset".equals(columnName)) {
                            offset = Bytes.toLong(keyValue.getValue());
                            logger.warn("列族:" + new String(keyValue.getFamily())
                                    + " 列:" + new String(keyValue.getQualifier()) + ":"
                                    + offset);
                        }
                    }

                    //新的hbase获取数据写法 效果同上
                    for (Cell cell : result.rawCells()) {
                        //获取rowkey columnfamily 列名 值
                        String rowKey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                        String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                        String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                        String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                        System.out.println("rowKey = " + rowKey + ",family = " + family + ",colName = " + colName + ",value = " + value);
                        switch (colName) {
                            case "topic":
                                topic = value;
                                break;
                            case "partition":
                                partition = Integer.valueOf(value);
                                break;
                            case "offset":
                                offset = Long.valueOf(value);
                                break;
                            default:
                                break;
                        }
                    }

                    //从hbase中获取topic-partition -> offset
                    fromOffsets.put(new TopicPartition(topic, partition), offset);
                }

                stream = KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Assign(fromOffsets.keySet(), kafkaParams, fromOffsets));
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else {
            //如果不存在TopicOffset表，则从topic首位置开始消费
            stream = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(topicsSet, kafkaParams));


            //并创建TopicOffset表
            HTableDescriptor hbaseTable = new HTableDescriptor(TableName.valueOf(datatable));
            hbaseTable.addFamily(new HColumnDescriptor("topic_partition_offset"));
            admin.createTable(hbaseTable);
            logger.warn(datatable + "表已经成功创建!----------------");
        }

        //这段代码貌似没用
//        JavaDStream<String> jpds = stream.map(
//                new Function<ConsumerRecord<String, String>, String>() {
//                    @Override
//                    public String call(ConsumerRecord<String, String> record) {
//                        String value = "";
//                        try {
//                            value = record.value();
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                        return value;
//                    }
//                });


        stream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
            @Override
            public void call(JavaRDD<ConsumerRecord<String, String>> rdd) throws Exception {

                /**
                 * TODO 添加业务代码
                 */
                rdd.map(new Function<ConsumerRecord<String, String>, Object>() {
                    @Override
                    public Object call(ConsumerRecord<String, String> v1) throws Exception {
                        return null;
                    }
                });
                OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                for (OffsetRange offsetRange : offsetRanges) {
                    logger.warn("the topic is " + offsetRange.topic());
                    logger.warn("the partition is " + offsetRange.partition());
                    logger.warn("the fromOffset is " + offsetRange.fromOffset());
                    logger.warn("the untilOffset is " + offsetRange.untilOffset());
                    logger.warn("the object is " + offsetRange.toString());

                    // begin your transaction
                    // 为了保证业务的事务性，最好把业务计算结果和offset同时进行hbase的存储，这样可以保证要么都成功，要么都失败，最终从端到端体现消费精确一次消费的意境
                    // update results
                    // update offsets where the end of existing offsets matches the beginning of this batch of offsets
                    // assert that offsets were updated correctly
                    Table table = con.getTable(tableName);
                    //create rowkey => (topic_partition)
                    Put put = new Put(Bytes.toBytes(offsetRange.topic() + "_" + offsetRange.partition()));
                    //create column(column family = "topic_partition_offset",column = "topic", value = topic)
                    put.addColumn(Bytes.toBytes("topic_partition_offset"), Bytes.toBytes("topic"),
                            Bytes.toBytes(offsetRange.topic()));
                    put.addColumn(Bytes.toBytes("topic_partition_offset"), Bytes.toBytes("partition"),
                            Bytes.toBytes(offsetRange.partition()));
                    put.addColumn(Bytes.toBytes("topic_partition_offset"), Bytes.toBytes("offset"),
                            Bytes.toBytes(offsetRange.untilOffset()));
                    table.put(put);
                    logger.warn("add data Success!");
                    // end your transaction
                }
                logger.warn("the RDD records counts is " + rdd.count());
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }
}
