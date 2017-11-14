package com.ztesoft.zstream.pipeline

import com.alibaba.fastjson.JSONArray
import com.ztesoft.zstream.{JobConf, SparkUtil}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * 流合并
  *
  * @author Yuri
  */
class Join extends PipelineProcessor {

  /**
    * 数据转换处理
    *
    * @param input 输入数据
    * @return 处理后的结果集，键为输出表名
    */
  override def process(input: Option[DStream[Row]]): Option[DStream[Row]] = {
    val sparkSession = params("sparkSession").asInstanceOf[SparkSession]
    val jobConf = params("jobConf").asInstanceOf[JobConf]
    val cfg = conf.map(s => (s._1.toString, s._2))
    val outputTableName = cfg("outputTableName").toString
    val joinType = cfg.getOrElse("joinType", "").toString
    val tableNames = cfg("inputTableName").toString.split(",").map(t => t.trim)
    val joinColumn = cfg("joinColumn").asInstanceOf[JSONArray]
    val queryColumn = cfg("queryColumn").asInstanceOf[JSONArray]
    val sourceDStreams = params.getOrElse("_sourceDStreams", scala.collection.mutable.Map[String, DStream[Row]]())
      .asInstanceOf[scala.collection.mutable.Map[String, DStream[Row]]]
    val leftDStream = sourceDStreams(tableNames(0))
    val rightDStream = sourceDStreams(tableNames(1))
    val leftPairDStream = leftDStream.map(row => (row.get(row.fieldIndex(joinColumn.getString(0))).toString, row))
    val rightPairDStream = rightDStream.map(row => (row.get(row.fieldIndex(joinColumn.getString(1))).toString, row))

    val joinDSteam = joinType match {
      case "left" =>
        leftPairDStream.leftOuterJoin(rightPairDStream).map(t => (Option(t._2._1), t._2._2))

      case "right" =>
        leftPairDStream.rightOuterJoin(rightPairDStream).map(t => (t._2._1, Option(t._2._2)))

      case _ =>
        leftPairDStream.join(rightPairDStream).map(t => (Option(t._2._1), Option(t._2._2)))
    }

    //合并2行为1行
    val queryColumns = new ArrayBuffer[(Int, String)]()
    for (qc <- queryColumn) {
      //qc: user.id
      val a = qc.toString.split("\\.").map(t => t.trim)
      //序号从1开始，与mergeRow中保持一致
      queryColumns.add((tableNames.indexOf(a(0)) + 1, a(1)))
    }
    val schema1 = SparkUtil.createSchema(jobConf.getTableDef.get(tableNames(0)))
    val schema2 = SparkUtil.createSchema(jobConf.getTableDef.get(tableNames(1)))
    val mergeDStream = joinDSteam.asInstanceOf[DStream[(Option[Row], Option[Row])]]
      .map(t => SparkUtil.mergeRow(t._1, t._2, schema1, schema2, queryColumns))

    //注册表
    val resultDSteam = mergeDStream.transform(rowRDD => {
      val colDef = jobConf.getTableDef.get(outputTableName)
      val schema = SparkUtil.createSchema(colDef)
      val df = sparkSession.createDataFrame(rowRDD, schema)
      df.createOrReplaceTempView(outputTableName)
      df.toJavaRDD
    })

    Option(resultDSteam)
  }
}
