package com.ztesoft.zstream.pipeline

import com.alibaba.fastjson.JSON
import com.ztesoft.zstream._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.JavaConversions._

/**
  * 数据源处理器
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
    val ssc = GlobalCache.ssc
    val jobConf = GlobalCache.jobConf

    val cfg = conf.map(s => (s._1.toString, s._2.toString))
    val subType = cfg("subType")
    //json或分隔符
    var format = cfg.getOrElse("format", ",")
    if(format.equals("|")) {
      format = "\\|"
    }
    val outputTableName = cfg("outputTableName")
    val colDef = jobConf.getTableDef.get(outputTableName)
    val defaultExtClass = "com.ztesoft.zstream.DefaultSourceExtProcessor"
    var extClass = cfg.getOrElse("extClass", "")
    if(extClass.isEmpty) {
      extClass = defaultExtClass
      Logging.logWarning(s"数据源${outputTableName}使用默认的扩展处理器$defaultExtClass")
    }

    val dstream = subType match {
      case "socket" =>
        val host = cfg("host")
        val port = cfg("port").toInt
        ssc.socketTextStream(host, port)

      case "file" =>
        //文件模式，默认hdfs格式，本地文件必须以file://开头
        val filePath = cfg("path")
        ssc.receiverStream(new FileReceiver(filePath))

      case "kafka" =>
        val Array(zkQuorum, group, topics, numThreads) = Array(cfg("zkQuorum"), cfg("group"), cfg("topics"), cfg.getOrElse("numThreads", "1"))
        val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
        KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

      case "directory" =>
        val path = cfg.getOrElse("path", "")
        val fileFilterFunc = new Function[Path, Boolean] {
          def apply(path: Path): Boolean = {
            SourceETL.filterFile(path.getName, extClass)
          }
        }
        ssc.fileStream[LongWritable, Text, TextInputFormat](path, fileFilterFunc, newFilesOnly = true).map(_._2.toString)

      case _ =>
        require(requirement = false, "不支持的数据源类型：" + subType)
        null
    }

    //经etl后的dstream
    val etlDStream = dstream.filter(line => SourceETL.filterLine(line, format, SparkUtil.createColumnDefList(colDef), extClass))
      .map(line => SourceETL.transformLine(line, format, SparkUtil.createColumnDefList(colDef), extClass))
    val schema = SparkUtil.createSchema(colDef)
    val ds = format match {
      case "json" =>
        etlDStream.map(line => SparkUtil.jsonObjectToRow(JSON.parseObject(line), schema))
      case _ =>
        etlDStream.map(_.split(format)).map(a => SparkUtil.arrayToRow(a, schema))
    }

    //数据源需要创建对应的表，这样后续就可以直接用了
    val result = ds.transform(rowRDD => {
      val sparkSession = SparkSession.builder().config(rowRDD.sparkContext.getConf).getOrCreate()
      val df = sparkSession.createDataFrame(rowRDD, SparkUtil.createSchema(colDef))
      df.createOrReplaceTempView(outputTableName)
      df.toJavaRDD
    })

    //缓存所有数据源对应的DStream
    GlobalCache.sourceDStreams.put(outputTableName, result)

    Option(result)
  }
}
