package com.ztesoft.zstream.pipeline

import java.util

import com.ztesoft.zstream._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._

/**
  * pipeline形式处理数据，有窗口和流合并时使用
  *
  * @author Yuri
  */
class SparkStreamPipelineStrategy(jobConf: JobConf) extends StreamStrategy {

  override def start(): Unit = {

    println(jobConf)
    val params = jobConf.getParams
    val master = params.getOrElse("master", "local[4]").toString
    val appName = jobConf.getName
    val duration = params.getOrElse("duration", "1").toString.toInt

    val conf = new SparkConf().setMaster(master).setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(duration))
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    val globalParams = scala.collection.mutable.Map[String, Any]("sparkSession" -> sparkSession, "ssc" -> ssc,
      "jobConf" -> jobConf
    )

    //数据源
    val sourceProcessors = jobConf.getSourceProcessors
    for (sp <- sourceProcessors) {
      processCurrent(sp, null, globalParams)
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def getNextConfs(conf: java.util.Map[String, Object]): java.util.List[java.util.Map[String, Object]] = {
    val confs = new util.ArrayList[java.util.Map[String, Object]]()
    val outputTableName = conf.getOrElse("outputTableName", "").toString
    if (outputTableName.isEmpty) {
      return null
    }
    for (c <- jobConf.getProcessors) {
      val inputTableName = c.getOrElse("inputTableName", "")
      if (inputTableName.equals(outputTableName)) {
        confs.add(c)
      }
    }
    confs
  }

  def processCurrent(conf: java.util.Map[String, Object], input: DStream[Row], globalParams: scala.collection.mutable.Map[String, Any]): DStream[Row] = {
    val pType = conf.get("type").toString
    val result = pType match {
      case "source" =>
        val source = new Source()
        source.init(conf, globalParams)
        source.process(null).get

      case "transform" =>
        val transform = new Transform()
        transform.init(conf, globalParams)
        transform.process(Option(input)).get

      case "action" =>
        val action = new Action()
        action.init(conf, globalParams)
        action.process(Option(input))
        null
    }

    processNext(conf, result, globalParams)

    result

  }

  def processNext(conf: java.util.Map[String, Object], input: DStream[Row], globalParams: scala.collection.mutable.Map[String, Any]): Unit = {
    val nextConfs = getNextConfs(conf)
    if (nextConfs != null) {
      for (nextConf <- nextConfs) {
        processCurrent(nextConf, input, globalParams)
      }
    }

  }

  override def stop(): Unit = {

  }
}

object SparkStreamPipelineStrategy {
  def main(args: Array[String]) {
    //参数设置和验证
    val param = new JobParam(args)
    val strategy = create(param.jobConf)
    strategy.start()
  }

  def create(jobConf: JobConf): SparkStreamPipelineStrategy = {
    new SparkStreamPipelineStrategy(jobConf)
  }
}
