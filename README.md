#### emr flink iceberg 
```shell
# emr 6.6.0 iceberg 0.13.2

# build, 注意build带上 -Dscope.type=provided 
mvn clean package  -Dscope.type=provided  -DskipTests 
# 编译好jar https://dxs9dnjebzm6y.cloudfront.net/tmp/emr-flink-iceberg-1.0-SNAPSHOT.jar

# emr submit job
# 使用Glue Catalog, 会在glue中创建表，kafka中json两个字段 id ,name 例如：{"id":1,"name":"customer"}
sudo flink run -m yarn-cluster \
-yjm 1024 -ytm 2048 -d \
-ys 4 -p 8 \
-c  com.aws.analytics.Kafka2Iceberg  \
/home/hadoop/emr-flink-iceberg-1.0-SNAPSHOT.jar \
-b b-2.xxxxx.kafka.ap-southeast-1.amazonaws.com:9092 \
-s xxxx_topic_001 \
-c s3://xxx/flink/checkpoint/ \
-g msk_group_01 \
-l 60 \
-t iceberg_tb_01 \
-w s3://xxxx/iceberg/ 

# 参数说明
Kafka2Iceberg 1.0
Usage: Kafka2Iceberg [options]

  -c, --checkpointDir <value>
                           checkpoint dir
  -l, --checkpointInterval <value>
                           checkpoint interval: default 60 seconds
  -b, --brokerList <value>
                           kafka broker list,sep comma
  -s, --sourceTopic <value>
                           kafka source topic
  -g, --groupId <value>    consumer group id
  -t, --tableName <value>  iceberg glue tableName
  -w, --warehouse <value>  iceberg s3 warehouse path

```
#### 截图
![](https://pcmyp.oss-accelerate.aliyuncs.com/markdown/Apache_Flink_Web_Dashboard.png)
![](https://pcmyp.oss-accelerate.aliyuncs.com/markdown/AWS_Glue_Console.png)
![](https://pcmyp.oss-accelerate.aliyuncs.com/markdown/chaopan_08f8bc6ad936__.png)
