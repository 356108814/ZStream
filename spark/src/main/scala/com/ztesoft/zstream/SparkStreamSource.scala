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
      val sourceType = cfg("sourceType")
      val path = cfg.getOrElse("path", "")
      val colDef = cfg("colDef")
      //改成格式，因为有分隔符和json这种
      val separator = cfg.getOrElse("separator", ",")
      val tableName = cfg("tableName")

      val dstream = sourceType match {
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

        case "jdbc" =>
          val url = cfg("url")
          val db = cfg("db")
          val table = cfg("table")
          val username = cfg("username")
          val password = cfg("password")
          val jdbcDF = sparkSession.read
            .format("jdbc")
            .option("url", url)
            .option("dbtable", s"$db.$table")
            .option("user", username)
            .option("password", password)
            .load()
          jdbcDF.createOrReplaceTempView(tableName)
          null

        case "file" =>
          //com.databricks.spark.csv
          val format = cfg.getOrElse("format", "json") //默认json格式
          val df = sparkSession.read.format(format).options(
            //删除指定属性，返回新的属性map
            (cfg - "sourceType" - "format" - "path" - "colDef" - "separator" - "tableName")
              .map(f => (f._1.toString, f._2.toString))).load(path)
          df.createOrReplaceTempView(tableName)
          null

        case _ =>
          require(requirement = false, "不支持的数据源类型：" + sourceType)
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
