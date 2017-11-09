package com.ztesoft.zstream

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka._

/**
  * spark streaming 数据源
  *
  * @author Yuri
  * @create 2017-11-8 16:45
  */
class SparkStreamSource[DStream] extends Source[DStream] {

  private var params: Map[String, Any] = _
  private var sources: List[Map[String, Any]] = _

  override def init(sources: List[Map[String, Any]], params: Map[String, Any]): Unit = {
    this.sources = sources
    this.params = params
  }

  override def process(): List[DStream] = {
    val ssc = params.get("ssc").get.asInstanceOf[StreamingContext]
    val sparkSession = params.get("sparkSession").get.asInstanceOf[SparkSession]

    val dstreams = sources.map(source => {
      val cfg = source.map(s => (s._1.toString, s._2.toString))
      val format = cfg("format")
      val path = cfg.getOrElse("path", "")
      val colDef = cfg("colDef")
      val separator = cfg.getOrElse("separator", ",")
      val tableName = cfg("tableName")

      val dstream = format match {
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

        case "file" =>
          //com.databricks.spark.csv
          //TODO 文件处理
          val df = sparkSession.read.format("csv").options(
            //删除指定属性，返回新的属性map
            (cfg - "format" - "path" - "outputTable" - "data").map(f => (f._1.toString, f._2.toString))).load(path)
          df.createOrReplaceTempView(tableName)
          null

        case _ =>
          require(requirement = false, "不支持的数据源类型：" + format)
          null
      }
      if(dstream != null) {
        val schema = SparkUtil.createSchema(colDef)
        val ds = dstream.map(_.split(separator)).map(a => SparkUtil.arrayToRow(a, schema))
        val result = ds.transform(rowRDD => {
          val df = sparkSession.createDataFrame(rowRDD, SparkUtil.createSchema(colDef))
          df.createOrReplaceTempView(tableName)
          rowRDD.toJavaRDD
        })
        //必须返回result而不是ds，否则后续操作找不到表，因为操作需要在一个线程内
        result
      }
    })
    dstreams.filter(f => f != null).asInstanceOf[List[DStream]]
  }
}
