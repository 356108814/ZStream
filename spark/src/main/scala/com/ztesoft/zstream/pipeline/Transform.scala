package com.ztesoft.zstream.pipeline

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

import scala.collection.JavaConversions._

/**
  * 转换
  *
  * @author Yuri
  */
class Transform extends PipelineProcessor {
  /**
    * 数据转换处理
    *
    * @param input 输入数据
    * @return 处理后的结果集，键为输出表名
    */
  override def process(input: Option[DStream[Row]]): Option[DStream[Row]] = {
    val sparkSession = params("sparkSession").asInstanceOf[SparkSession]
    val cfg = conf.map(s => (s._1.toString, s._2.toString))
    val sql = cfg("sql")
    val outputTableName = cfg("outputTableName")
    var dstream = input.get

    val windowDuration = cfg.getOrElse("windowDuration", "0").toInt
    val slideDuration = cfg.getOrElse("slideDuration", "0").toInt
    if (windowDuration > 0 && slideDuration > 0) {
      dstream = dstream.window(Seconds(windowDuration), Seconds(slideDuration))
    } else if (windowDuration > 0) {
      dstream = dstream.window(Seconds(windowDuration))
    }

    val result = dstream.transform(rowRDD => {
      val df = sparkSession.sql(sql)
      df.createOrReplaceTempView(outputTableName)
      df.show()
      df.toJavaRDD
    })

    Option(result)
  }
}
