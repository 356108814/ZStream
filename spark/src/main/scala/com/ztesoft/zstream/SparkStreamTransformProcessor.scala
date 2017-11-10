package com.ztesoft.zstream

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * spark数据转换
  *
  * @author Yuri
  * @create 2017-11-10 10:28
  */
class SparkStreamTransformProcessor[T] extends TransformProcessor[T] {

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
    val sparkSession = params.get("sparkSession").get.asInstanceOf[SparkSession]
    confList.map(conf => {
      val cfg = conf.map(s => (s._1.toString, s._2.toString))
      val sql = cfg("sql")
      val tableName = cfg("tableName")

      //创建临时表
      val createTable = () => {
        val df = sparkSession.sql(sql)
        df.createOrReplaceTempView(tableName)
      }

      //将所有创建临时表的函数缓存起来，统一在action中执行，这样所有表就可以共享了
      val createTableFuncList = params.getOrElse("_createTableFuncList", ArrayBuffer[() => Unit]()).asInstanceOf[ArrayBuffer[() => Unit]]
      createTableFuncList += createTable
      params.put("_createTableFuncList", createTableFuncList)

    })

    input
  }
}
