package com.ztesoft.zstream.pipeline

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream

import scala.collection.JavaConversions._

/**
  * 输出动作
  *
  * @author Yuri
  */
class Action extends PipelineProcessor {
  /**
    * 数据转换处理
    *
    * @param input 输入数据
    * @return 处理后的结果集，键为输出表名
    */
  override def process(input: Option[DStream[Row]]): Option[DStream[Row]] = {
    val sparkSession = params("sparkSession").asInstanceOf[SparkSession]
    val dstream = input.get
    dstream.foreachRDD(rowRDD => {
      val cfg = conf.map(s => (s._1.toString, s._2.toString))
      val subType = cfg("subType")
      val inputTableName = cfg("inputTableName")
      val df = sparkSession.table(inputTableName)

      subType match {
        case "console" =>
          df.show()
        case _ =>
          df.show()
      }
    })
    null
  }
}
