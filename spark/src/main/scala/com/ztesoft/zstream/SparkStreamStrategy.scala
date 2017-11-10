package com.ztesoft.zstream

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.{Map => MMap}

/**
  * spark
  *
  * @author Yuri
  * @create 2017-11-7 17:10
  */
class SparkStreamStrategy(jobConf: JobConf) extends DefaultStreamStrategy with StreamStrategy {

  override def start(): Unit = {

    println(jobConf)

    val conf = new SparkConf().setMaster(jobConf.getParams.get("master").toString).setAppName(jobConf.getName)
    val ssc = new StreamingContext(conf, Seconds(5))
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    val params = MMap[String, Any]("sparkSession" -> sparkSession, "ssc" -> ssc)

    val colDef1 = "[{\"name\": \"id\", \"type\": \"int\"}, {\"name\": \"name\", \"type\": \"string\"}]"
    val colDef2 = "[{\"name\": \"id\", \"type\": \"int\"}, {\"name\": \"age\", \"type\": \"int\"}]"
    val s1 = Map("sourceType" -> "socket", "path" -> "", "tableName" -> "user", "colDef" -> colDef1, "host" -> "10.45.47.66", "port" -> 9999)
    val s2 = Map("sourceType" -> "file", "path" -> "J:/spark/source/user_rel.txt", "tableName" -> "user_rel", "colDef" -> colDef2, "format" -> "json")
    val s3 = Map("sourceType" -> "socket", "path" -> "", "tableName" -> "user_rel", "colDef" -> colDef2, "host" -> "localhost", "port" -> 9998)

    //数据源
    val sources = List(s1, s2)
    val sourceProcessor = new SparkStreamSourceProcessor()
    sourceProcessor.init(sources, params)
    var result = sourceProcessor.process(List())

    //数据转换
    val t1 = Map("sql" -> "select * from user where id > 3", "tableName" -> "adult")
    val transformProcessor = new SparkStreamTransformProcessor()
    transformProcessor.init(List(t1), params)
    result = transformProcessor.process(result)

    //数据操作
    val a1 = Map("inputTableName" -> "adult")
    val actionProcessor = new SparkStreamActionProcessor()
    actionProcessor.init(List(a1), params)
    actionProcessor.process(result)

    ssc.start()
    ssc.awaitTermination()
  }

  override def stop(): Unit = {

  }
}

object SparkStreamStrategy {
  def main(args: Array[String]) {
    //参数设置和验证
    val param = new JobParam(args)
    val strategy = create(param.jobConf)
    strategy.start()
  }

  def create(jobConf: JobConf): SparkStreamStrategy = {
    new SparkStreamStrategy(jobConf)
  }
}
