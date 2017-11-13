package com.ztesoft.zstream.pipeline

import com.ztesoft.zstream._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._

/**
  *
  * @author Yuri
  */
class SparkStreamPipelineStrategy(jobConf: JobConf) extends StreamStrategy {
  override def start(): Unit = {

    println(jobConf)
    val params = jobConf.getParams
    val master = params.getOrElse("master", "local[4]").toString
    val appName = jobConf.getName
    val duration = params.getOrElse("duration", "5").toString.toInt

    val conf = new SparkConf().setMaster(master).setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(duration))
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    val globalParams = scala.collection.mutable.Map[String, Any]("sparkSession" -> sparkSession, "ssc" -> ssc)

    //数据源
    val sourceProcessors = jobConf.getSourceProcessors
    for (sp <- sourceProcessors) {
      val source = new Source()
      source.init(sp, globalParams)
      val dstream = source.process(null)
      val nextConf = getNextConf(sp)
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def getNextConf(conf: java.util.Map[String, Object]): java.util.Map[String, Object] = {
    val outputTableName = conf.getOrElse("outputTableName", "").toString
    if (outputTableName.isEmpty) {
      return null
    }
    for (c <- jobConf.getProcessors) {
      val inputTableName = c.getOrElse("inputTableName", "")
      if (inputTableName.equals(outputTableName)) {
        return c
      }
    }
    null
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
