package com.aws.analytics

import com.aws.analytics.conf.Config

import java.util.Properties
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}



object Kafka2Iceberg {


  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val params = Config.parseConfig(Kafka2Iceberg, args)
    env.enableCheckpointing(params.checkpointInterval.toInt * 1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    val rocksBackend: StateBackend = new RocksDBStateBackend(params.checkpointDir)
    env.setStateBackend(rocksBackend)

    val settings = EnvironmentSettings.newInstance.inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env,settings)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", params.brokerList)
    properties.setProperty("group.id", params.groupId)


    val kafkaSQL=
      s"""
        |CREATE TABLE kafka_table (
        |  `id` int,
        |  `name` string
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = '${params.sourceTopic}',
        |  'properties.bootstrap.servers' = '${params.brokerList}',
        |  'properties.group.id' = '${params.groupId}',
        |  'scan.startup.mode' = 'latest-offset',
        |  'format' = 'json'
        |)
        |""".stripMargin

    val catalogSQL=
      s"""arm
        |CREATE CATALOG my_glue_catalog WITH (
        |  'type'='iceberg',
        |  'warehouse'='${params.warehouse}',
        |  'catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog',
        |  'io-impl'='org.apache.iceberg.aws.s3.S3FileIO'
        |)
        |""".stripMargin
    val tableSql =
      """
        |
        |CREATE TABLE if not exists `my_glue_catalog`.`default`.`iceberg_tb_01` (
        |    id BIGINT COMMENT 'unique id',
        |    data STRING
        |)
        |""".stripMargin

    val insertSQL =
      s"""
        |insert into `my_glue_catalog`.`default`.`${params.tableName}` select * from kafka_table
        |""".stripMargin
    tEnv.executeSql(kafkaSQL)
    tEnv.executeSql(catalogSQL)
    tEnv.executeSql(tableSql)
    tEnv.executeSql(insertSQL)

  }
}
