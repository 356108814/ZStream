package com.ztesoft.zstream

import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * spark数据转换
  *
  * @author Yuri
  */
class SparkStreamTransformProcessor[T] extends TransformProcessor[T] {

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
    val sparkSession = params("sparkSession").asInstanceOf[SparkSession]
    confList.map(conf => {
      val cfg = conf.map(s => (s._1.toString, s._2.toString))
      val sql = cfg("sql")
      val outputTableName = cfg("outputTableName")

      //创建临时表
      val createTable = () => {
        val df = sparkSession.sql(sql)
        df.createOrReplaceTempView(outputTableName)
      }

      //将所有创建临时表的函数缓存起来，统一在action中执行，这样所有表就可以共享了
      val createTableFuncList = params.getOrElse("_createTableFuncList", ArrayBuffer[() => Unit]()).asInstanceOf[ArrayBuffer[() => Unit]]
      createTableFuncList += createTable
      params.put("_createTableFuncList", createTableFuncList)

    })

    input
  }
}
