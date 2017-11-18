package com.ztesoft.zstream

import org.apache.spark.sql.Row
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
  * 全局参数
  *
  * @author Yuri
  */
object GlobalCache {
  var ssc: StreamingContext = _
  var jobConf: JobConf = _
  var sourceDStreams: scala.collection.mutable.Map[String, DStream[Row]] = scala.collection.mutable.Map[String, DStream[Row]]()

  private val paramMap = scala.collection.mutable.Map[String, Any]()

  /**
    * 设置参数
    *
    * @param name  参数名称
    * @param value 参数值
    */
  def putParam(name: String, value: Any): Unit = {
    paramMap.put(name, value)
  }

  /**
    * 获取参数
    *
    * @param name 参数名称
    * @return 参数值
    */
  def getParam(name: String): Any = {
    paramMap.get(name)
  }

}
