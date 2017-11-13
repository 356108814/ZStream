package com.ztesoft.zstream

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.{Map => MMap}
import scala.collection.JavaConversions._

/**
  * spark
  *
  * @author Yuri
  */
class SparkStreamStrategy(jobConf: JobConf) extends StreamStrategy {

  override def start(): Unit = {

    println(jobConf)
    val params = jobConf.getParams
    val master = params.getOrElse("master", "local[4]").toString
    val appName = jobConf.getName
    val duration = params.getOrElse("duration", "5").toString.toInt

    val conf = new SparkConf().setMaster(master).setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(duration))
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    val globalParams = MMap[String, Any]("sparkSession" -> sparkSession, "ssc" -> ssc)

    //数据源
    val sourceProcessor = new SparkStreamSourceProcessor()
    sourceProcessor.init(jobConf.getSourceProcessors, globalParams)
    var result = sourceProcessor.process(new util.ArrayList[Nothing]())

    //数据转换
    val transformProcessor = new SparkStreamTransformProcessor()
    transformProcessor.init(jobConf.getTransformProcessors, globalParams)
    result = transformProcessor.process(result)

    //数据操作
    val actionProcessor = new SparkStreamActionProcessor()
    actionProcessor.init(jobConf.getProcessors, globalParams)
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
