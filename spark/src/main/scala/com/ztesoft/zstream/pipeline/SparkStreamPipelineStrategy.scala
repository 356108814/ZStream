package com.ztesoft.zstream.pipeline

import java.util

import com.ztesoft.zstream._
//import com.ztesoft.zstream.common.KerberosUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._

/**
  * pipeline形式处理数据，有窗口和流合并时使用
  *
  * @author Yuri
  */
class SparkStreamPipelineStrategy(jobConf: JobConf) extends StreamStrategy {

  private val processedMap = scala.collection.mutable.Map[java.util.Map[String, Object], Boolean]()

  /**
    * 使用checkpoint时，流计算业务必须包含在创建StreamingContext函数中
    */
  def createSSC(): StreamingContext = {
    val master = params.getOrElse("master", "local[4]").toString
    val appName = jobConf.getName
    val duration = params.getOrElse("duration", "5").toString.toInt

    val sparkConf = new SparkConf().setMaster(master).setAppName(appName)
    val ssc = new StreamingContext(sparkConf, Seconds(duration))
    ssc.sparkContext.getConf


    GlobalCache.ssc = ssc
    GlobalCache.jobConf = jobConf

    //先处理数据源，缓存所有流
    processSource()
    //再处理所有数据源的下一级
    val sourceProcessors = jobConf.getSourceProcessors
    for (conf <- sourceProcessors) {
      val input = GlobalCache.sourceDStreams(conf.get("outputTableName").toString)
      processNext(conf, input)
    }

    ssc
  }

  override def start(): Unit = {
    //    KerberosUtil.loginCluster(true, true)
    //TODO checkpoint需要根据是否有acc来判断
    val checkpoint = params.getOrDefault("checkpoint", "").toString
    val ssc = {
      if (checkpoint.isEmpty) {
        createSSC()
      } else {
        val ssc = StreamingContext.getOrCreate(checkpoint, createSSC)
        ssc.checkpoint(checkpoint)
        ssc
      }
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
    */
  def processSource(): Unit = {
    val sourceProcessors = jobConf.getSourceProcessors
    for (sp <- sourceProcessors) {
      val source = new Source()
      source.init(sp)
      source.process(null)
      processedMap.put(sp, true)
    }
  }

  def processCurrent(conf: java.util.Map[String, Object], input: DStream[Row]): DStream[Row] = {
    if (processedMap.contains(conf)) {
      return null
    }
    val pType = conf.get("type").toString
    val result = pType match {
      case "source" =>
        val source = new Source()
        source.init(conf)
        source.process(null).get

      case "join" =>
        val join = new Join()
        join.init(conf)
        join.process(null).get

      case "transform" =>
        val transform = new Transform()
        transform.init(conf)
        transform.process(Option(input)).get

      case "action" =>
        val action = new Action()
        action.init(conf)
        action.process(Option(input))
        null
    }
    processedMap.put(conf, true)

    processNext(conf, result)

    result

  }

  def processNext(conf: java.util.Map[String, Object], input: DStream[Row]): Unit = {
    val nextConfs = getNextConfs(conf)
    if (nextConfs != null) {
      for (nextConf <- nextConfs) {
        processCurrent(nextConf, input)
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
    val strategy = new SparkStreamPipelineStrategy(jobConf)
    strategy.init(jobConf)
    strategy
  }
}
