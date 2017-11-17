package com.ztesoft.zstream.pipeline

import java.util

import com.ztesoft.zstream._
import com.ztesoft.zstream.common.KerberosUtil
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

  val processedMap = scala.collection.mutable.Map[java.util.Map[String, Object], Boolean]()

  override def start(): Unit = {

    println(jobConf)
    val params = jobConf.getParams
    val master = params.getOrElse("master", "local[4]").toString
    val appName = jobConf.getName
    val duration = params.getOrElse("duration", "5").toString.toInt

    KerberosUtil.loginCluster(true, true)

    val conf = new SparkConf().setMaster(master).setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(duration))
//    ssc.checkpoint("J:\\spark\\checkpoint")
    ssc.checkpoint("/tmp/checkpoint")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    val globalParams = scala.collection.mutable.Map[String, Any]("sparkSession" -> sparkSession, "ssc" -> ssc,
      "jobConf" -> jobConf
    )

    //先处理数据源，缓存所有流
    processSource(globalParams)
    //再处理所有数据源的下一级
    val sourceDStreams = globalParams.getOrElse("_sourceDStreams", scala.collection.mutable.Map[String, DStream[Row]]())
      .asInstanceOf[scala.collection.mutable.Map[String, DStream[Row]]]
    val sourceProcessors = jobConf.getSourceProcessors
    for (sp <- sourceProcessors) {
      processNext(sp, sourceDStreams(sp.get("outputTableName").toString), globalParams)
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
      val inputTableName = c.getOrElse("inputTableName", "").toString.split(",").map(name => name.trim)
      if (inputTableName.contains(outputTableName)) {
        confs.add(c)
      }
    }
    confs
  }

  /**
    * 处理数据源
    *
    * @param globalParams 全局参数
    */
  def processSource(globalParams: scala.collection.mutable.Map[String, Any]): Unit = {
    val sourceProcessors = jobConf.getSourceProcessors
    for (sp <- sourceProcessors) {
      val source = new Source()
      source.init(sp, globalParams)
      source.process(null)
      processedMap.put(sp, true)
    }
  }

  def processCurrent(conf: java.util.Map[String, Object], input: DStream[Row], globalParams: scala.collection.mutable.Map[String, Any]): DStream[Row] = {
    if (processedMap.contains(conf)) {
      return null
    }
    val pType = conf.get("type").toString
    val result = pType match {
      case "source" =>
        val source = new Source()
        source.init(conf, globalParams)
        source.process(null).get

      case "join" =>
        val join = new Join()
        join.init(conf, globalParams)
        join.process(null).get

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
    processedMap.put(conf, true)

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
