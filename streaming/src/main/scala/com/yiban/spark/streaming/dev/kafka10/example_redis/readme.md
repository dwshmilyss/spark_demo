##### 数据源格式
```$xslt
2018-02-22T00:00:00+08:00|~|200|~|/test?pcid=DEIBAH&siteid=3

2018-02-22T00:00:00+08:00|~|200|~|/test?pcid=GLLIEG&siteid=3

2018-02-22T00:00:00+08:00|~|200|~|/test?pcid=HIJMEC&siteid=8

2018-02-22T00:00:00+08:00|~|200|~|/test?pcid=HMGBDE&siteid=3

2018-02-22T00:00:00+08:00|~|200|~|/test?pcid=HIJFLA&siteid=4

2018-02-22T00:00:01+08:00|~|200|~|/test?pcid=JCEBBC&siteid=9

2018-02-22T00:00:01+08:00|~|200|~|/test?pcid=KJLAKG&siteid=8

2018-02-22T00:00:01+08:00|~|200|~|/test?pcid=FHEIKI&siteid=3

2018-02-22T00:00:01+08:00|~|200|~|/test?pcid=IGIDLB&siteid=3

2018-02-22T00:00:01+08:00|~|200|~|/test?pcid=IIIJCD&siteid=5
```
---
##### 运行参数
```$xslt
./spark-submit \
--class com.lxw1234.spark.TestSparkStreaming \
--master local[2] \
--conf spark.streaming.kafka.maxRatePerPartition=20000 \
--jars /data1/home/dmp/lxw/realtime/commons-pool2-2.3.jar,\
/data1/home/dmp/lxw/realtime/jedis-2.9.0.jar,\
/data1/home/dmp/lxw/realtime/kafka-clients-0.11.0.1.jar,\
/data1/home/dmp/lxw/realtime/spark-streaming-kafka-0-10_2.11-2.2.1.jar \
/data1/home/dmp/lxw/realtime/testsparkstreaming.jar \
--executor-memory 4G \
--num-executors 1
```