package com.ztesoft.zstream.action

import java.util.Date

import com.alibaba.fastjson.JSONObject
import com.ztesoft.zstream.GlobalCache
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.JavaConversions._

/**
  * 调试输出
  *
  * @author Yuri
  */
class DebugAction {
  var id: String = ""
  var jobId: String = ""
  var inputTableName: String = ""
  var url: String = ""
  var dbtable: String = ""

  def init(id: String, jobId: String, inputTableName: String, url: String, dbtable: String): Unit = {
    this.id = id
    this.jobId = jobId
    this.inputTableName = inputTableName
    this.url = url
    this.dbtable = dbtable
  }

  def process(df: DataFrame): Unit = {
    val rows = df.collect()
    if (rows.isEmpty) {
      return
    }
    val result = getFormatResult(df.schema, rows)
    val row = Row.fromSeq(List(id, jobId, inputTableName, result, new java.sql.Timestamp(new Date().getTime)))
    val schema = StructType(List(
      StructField("id", DataTypes.StringType, nullable = false),
      StructField("task_id", DataTypes.StringType, nullable = false),
      StructField("table_name", DataTypes.StringType, nullable = false),
      StructField("result", DataTypes.StringType, nullable = true),
      StructField("create_date", DataTypes.TimestampType, nullable = true)
    ))
    val list = new java.util.ArrayList[Row]()
    list.add(row)
    val resultDF = df.sparkSession.createDataFrame(list, schema)
    resultDF.write.mode("append")
      .format("jdbc")
      .option("url", url)
      .option("dbtable", dbtable)
      .save()

    GlobalCache.ssc.stop()
  }

  def getFormatResult(schema: StructType, rows: Array[Row]): String = {
    val result = new JSONObject()
    val columns: java.util.List[String] = schema.map(s => s.name)
    val values: java.util.List[JSONObject] = rows.map(row => {
      var i = 0
      val obj = new JSONObject()
      for (s <- schema.toList) {
        obj.put(s.name, row.get(i))
        i += 1
      }
      obj
    }).toList
    result.put("columns", columns)
    result.put("values", values)
    result.toJSONString
  }
}
