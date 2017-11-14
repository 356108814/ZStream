package com.ztesoft.zstream.pipeline

import com.ztesoft.zstream.{JobConf, SparkUtil}
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
    val jobConf = params("jobConf").asInstanceOf[JobConf]
    val cfg = conf.map(s => (s._1.toString, s._2.toString))
    val sql = cfg("sql")
    val inputTableName = cfg("inputTableName")
    val outputTableName = cfg("outputTableName")
    val dstream = input.get

    val windowDuration = cfg.getOrElse("windowDuration", "0").toInt
    val slideDuration = cfg.getOrElse("slideDuration", "0").toInt

    val windowDStream = {
      if (windowDuration > 0 && slideDuration > 0) {
        dstream.window(Seconds(windowDuration), Seconds(slideDuration))
      } else if (windowDuration > 0) {
        dstream.window(Seconds(windowDuration))
      } else {
        dstream
      }
    }

    val result = windowDStream.transform(rowRDD => {
      val colDef = jobConf.getTableDef.get(inputTableName)
      val schema = SparkUtil.createSchema(colDef)
      val df = sparkSession.createDataFrame(rowRDD, schema)
      df.createOrReplaceTempView(inputTableName)
      //根据查询结果创建新表
      val returnDF = sparkSession.sql(sql)
      returnDF.createOrReplaceTempView(outputTableName)
      returnDF.toJavaRDD
    })

    Option(result)
  }
}
