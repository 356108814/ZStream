package com.ztesoft.zstream

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ArrayBuffer

/**
  * spark操作
  *
  * @author Yuri
  * @create 2017-11-10 14:24
  */
class SparkStreamActionProcessor[T] extends ActionProcessor[T] {

  private var confList: List[Map[String, Any]] = _
  private var params: scala.collection.mutable.Map[String, Any] = _

  override def init(confList: List[Map[String, Any]], params: scala.collection.mutable.Map[String, Any]): Unit = {
    this.confList = confList
    this.params = params
  }

  /**
    * 数据转换处理
    *
    * @param input 输入数据
    * @return 处理后的结果集，键为输出表名
    */
  override def process(input: List[(String, T)]): List[(String, T)] = {
    val dstream = input.head.asInstanceOf[(String, DStream[Row])]._2
    dstream.foreachRDD(rowRDD => {
      val createTableFuncList = params.getOrElse("_createTableFuncList", ArrayBuffer[() => Unit]()).asInstanceOf[ArrayBuffer[() => Unit]]
      createTableFuncList.foreach(func => func())
      
      confList.foreach(source => {
        val sparkSession = params.get("sparkSession").get.asInstanceOf[SparkSession]
        val cfg = source.map(s => (s._1.toString, s._2.toString))
        val inputTableName = cfg("inputTableName")
        val df = sparkSession.table(inputTableName)
        df.show()
      })
    })
    List()
  }
}
