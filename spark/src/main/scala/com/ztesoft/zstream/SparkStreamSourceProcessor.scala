package com.ztesoft.zstream

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka._

/**
  * spark streaming 数据源
  *
  * @author Yuri
  * @create 2017-11-8 16:45
  */
class SparkStreamSourceProcessor[T] extends SourceProcessor[T] {

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
    val ssc = params.get("ssc").get.asInstanceOf[StreamingContext]
    val sparkSession = params.get("sparkSession").get.asInstanceOf[SparkSession]

    val dstreams = confList.map(source => {
      val cfg = source.map(s => (s._1.toString, s._2.toString))
      val sourceType = cfg("sourceType")
      val path = cfg.getOrElse("path", "")
      val colDef = cfg("colDef")
      //json或分隔符
      val format = cfg.getOrElse("format", ",")
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
      if (dstream != null) {
        val schema = SparkUtil.createSchema(colDef)
        val ds = format match {
          case "json" =>
            dstream.map(a => SparkUtil.jsonObjectToRow(JSON.parseObject(a), schema))
          case _ =>
            dstream.map(_.split(format)).map(a => SparkUtil.arrayToRow(a, schema))
        }

        //数据源需要创建对应的表，这样后续就可以直接用了
        val result = ds.transform(rowRDD => {
          val df = sparkSession.createDataFrame(rowRDD, SparkUtil.createSchema(colDef))
          df.createOrReplaceTempView(tableName)
          rowRDD.toJavaRDD
        })

        (tableName, result)
      } else {
        (tableName, null)
      }
    })
    dstreams.filter(t => t._2 != null).asInstanceOf[List[(String, T)]]
  }

}
