package com.ztesoft.zstream.pipeline

import com.ztesoft.zstream.GlobalCache
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream

import scala.collection.JavaConversions._

/**
  * 维度表处理器
  *
  * @author Yuri
  */
class Dim extends PipelineProcessor {
  var sparkConf: SparkConf = _

  def setSparkConf(sparkConf: SparkConf): Unit = {
    this.sparkConf = sparkConf
  }

  /**
    * 维度表处理，注册为表
    *
    * @param input 输入数据
    * @return 处理后的结果集，键为输出表名
    */
  override def process(input: Option[DStream[Row]]): Option[DStream[Row]] = {
    val cfg = conf.map(s => (s._1.toString, s._2.toString))
    val subType = cfg("subType")
    //json或分隔符
    val outputTableName = cfg("outputTableName")
    val sparkSession = SparkSession.builder().config(this.sparkConf).getOrCreate()

    subType match {
      case "jdbc" =>
        val url = cfg("url")
        val dbtable = cfg("dbtable")
        val username = cfg("username")
        val password = cfg("password")
        val jdbcDF = sparkSession.read
          .format("jdbc")
          .option("url", url)
          .option("dbtable", dbtable)
          .option("user", username)
          .option("password", password)
          .load()
        jdbcDF.createOrReplaceTempView(outputTableName)

      case "file" =>
        val path = cfg("path")
        //com.databricks.spark.csv，默认json格式，json、csv
        val format = cfg.getOrElse("format", "json")
        val df = sparkSession.read.format(format).load(path)
        df.createOrReplaceTempView(outputTableName)

      case _ =>
        require(requirement = false, "不支持的维度表输入类型：" + subType)
    }

    Option(null)
  }
}
