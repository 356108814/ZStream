package com.ztesoft.zstream.pipeline

import com.alibaba.fastjson.JSON
import com.ztesoft.zstream.{SourceETL, SparkUtil}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.JavaConversions._

/**
  *
  * @author Yuri
  */
class Source extends PipelineProcessor {

  /**
    * 数据转换处理
    *
    * @param input 输入数据
    * @return 处理后的结果集，键为输出表名
    */
  override def process(input: Option[DStream[Row]]): Option[DStream[Row]] = {
    val ssc = params("ssc").asInstanceOf[StreamingContext]
    val sparkSession = params("sparkSession").asInstanceOf[SparkSession]

    val cfg = conf.map(s => (s._1.toString, s._2.toString))
    val subType = cfg("subType")
    val path = cfg.getOrElse("path", "")
    val colDef = cfg("colDef")
    //json或分隔符
    val format = cfg.getOrElse("format", ",")
    val outputTableName = cfg("outputTableName")

    val dstream = subType match {
      case "socket" =>
        val host = cfg("host")
        val port = cfg("port").toInt
        ssc.socketTextStream(host, port)

      case "kafka" =>
        val Array(zkQuorum, group, topics, numThreads) = Array(cfg("zkQuorum"), cfg("group"), cfg("topics"), cfg.getOrElse("numThreads", "1"))
        val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
        KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

      case "directory" =>
        ssc.textFileStream(path)

      case _ =>
        require(requirement = false, "不支持的数据源类型：" + subType)
        null
    }

    //经etl后的dstream
    val extClass = cfg.getOrElse("extClass", "com.ztesoft.zstream.DefaultSourceExtProcessor")
    val etlDStream = dstream.filter(line => SourceETL.filter(line, format, SparkUtil.createColumnDefList(colDef), extClass))
      .map(line => SourceETL.transform(line, format, SparkUtil.createColumnDefList(colDef), extClass))
    val schema = SparkUtil.createSchema(colDef)
    val ds = format match {
      case "json" =>
        etlDStream.map(line => SparkUtil.jsonObjectToRow(JSON.parseObject(line), schema))
      case _ =>
        etlDStream.map(_.split(format)).map(a => SparkUtil.arrayToRow(a, schema))
    }

    //数据源需要创建对应的表，这样后续就可以直接用了
    val result = ds.transform(rowRDD => {
      val df = sparkSession.createDataFrame(rowRDD, SparkUtil.createSchema(colDef))
      df.createOrReplaceTempView(outputTableName)
      rowRDD.toJavaRDD
    })

    Option(result)
  }
}
