package com.ztesoft.zstream

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
  * spark操作
  *
  * @author Yuri
  */
class SparkStreamActionProcessor[T] extends ActionProcessor[T] {

  private var confList: java.util.List[java.util.Map[String, Object]] = _
  private var params: scala.collection.mutable.Map[String, Any] = _

  override def init(confList: java.util.List[java.util.Map[String, Object]], params: scala.collection.mutable.Map[String, Any]): Unit = {
    this.confList = confList
    this.params = params
  }

  /**
    * 数据转换处理
    *
    * @param input 输入数据
    * @return 处理后的结果集，键为输出表名
    */
  override def process(input: java.util.List[T]): java.util.List[T] = {
    val dstreams = input.get(0).asInstanceOf[ArrayBuffer[(String, DStream[Row])]]
    val dstream = dstreams.head._2
    dstream.foreachRDD(rowRDD => {
      val createTableFuncList = params.getOrElse("_createTableFuncList", ArrayBuffer[() => Unit]()).asInstanceOf[ArrayBuffer[() => Unit]]
      createTableFuncList.foreach(func => func())

      confList.foreach(source => {
        val sparkSession = params("sparkSession").asInstanceOf[SparkSession]
        val cfg = source.map(s => (s._1.toString, s._2.toString))
        val inputTableName = cfg("inputTableName")
        val df = sparkSession.table(inputTableName)
        df.show()
      })
    })
    List()
  }
}