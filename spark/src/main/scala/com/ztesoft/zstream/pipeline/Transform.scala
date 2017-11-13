package com.ztesoft.zstream.pipeline

import com.ztesoft.zstream.PipelineProcessor
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream

import scala.collection.JavaConversions._

/**
  * 转换
  *
  * @author Yuri
  */
class Transform[T] extends PipelineProcessor[T] {
  /**
    * 数据转换处理
    *
    * @param input 输入数据
    * @return 处理后的结果集，键为输出表名
    */
  override def process(input: Option[T]): Option[T] = {
    val sparkSession = params("sparkSession").asInstanceOf[SparkSession]
    val cfg = conf.map(s => (s._1.toString, s._2.toString))
    val sql = cfg("sql")
    val outputTableName = cfg("outputTableName")
    val dstream = input.asInstanceOf[DStream[Row]]
    val result = dstream.transform(rowRDD => {
      val df = sparkSession.sql(sql)
      df.createOrReplaceTempView(outputTableName)
      df.toJavaRDD
    })

    Option(result.asInstanceOf[T])
  }
}
