package com.ztesoft.zstream

import java.util.{List => JList, Map => JMap}

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark streaming 数据源
  *
  * @author Yuri
  * @create 2017-11-8 16:45
  */
class SparkStreamSource[DStream] extends Source[DStream] {

  private var params: Map[String, Any] = _
  private var sources: List[Map[Any, Any]] = _

  override def init(params: Map[String, Any]): Unit = {
    this.params = params
    sources = List(Map("format" -> "socket", "path" -> "", "tableName" -> "user"))
  }

  /**
    * 获取列定义
    */
  override def getDef: List[ColumnDef] = {
    val s = ""
    //    val
    List(new ColumnDef(1, "", ""))
  }

  override def process(): List[DStream] = {
    val ssc = params.get("ssc").asInstanceOf[StreamingContext]
    val sparkSession = params.get("sparkSession").asInstanceOf[SparkSession]

    val dstreams = sources.map(cfg => {
      val format = cfg.get("format").toString
      val path = cfg.get("path").toString
      val tableName = cfg.get("tableName").toString

      format match {
        case "socket" =>
          val lines = ssc.socketTextStream("localhost", 9999)
          val dStream = lines.map(_.split(",")).map(a => Row(a(0).toInt, a(1)))
          dStream.transform(rdd => {
            val structFields = List(StructField("id", IntegerType), StructField("name", StringType))
            //最后通过StructField的集合来初始化表的模式。
            val schema = StructType(structFields)
            val df = sparkSession.createDataFrame(rdd, schema)
            df.createOrReplaceTempView(tableName)
            df.toJavaRDD
          })

        case "kafka" =>


        case _ =>
          //默认文件形式
          val lines = ssc.textFileStream(path)
          val dStream = lines.map(_.split(",")).map(a => Row(a(0).toInt, a(1)))
          null

      }
    })

    List(dstreams).asInstanceOf[List[DStream]]
  }


}
