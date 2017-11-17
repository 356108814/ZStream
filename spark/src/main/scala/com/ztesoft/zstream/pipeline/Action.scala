package com.ztesoft.zstream.pipeline

import java.io.FileWriter
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.ztesoft.zstream.common.KerberosUtil
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream

import scala.collection.JavaConversions._

/**
  * 输出动作
  *
  * @author Yuri
  */
class Action extends PipelineProcessor {
  /**
    * 数据转换处理
    *
    * @param input 输入数据
    * @return 处理后的结果集，键为输出表名
    */
  override def process(input: Option[DStream[Row]]): Option[DStream[Row]] = {
    val sparkSession = params("sparkSession").asInstanceOf[SparkSession]
    val dstream = input.get
    val cfg = conf.map(s => (s._1.toString, s._2.toString))
    val subType = cfg("subType")
    val inputTableName = cfg("inputTableName")
    val sql = cfg("sql")

    dstream.foreachRDD(rowRDD => {
      val df = {
        if (sql.isEmpty) {
          sparkSession.table(inputTableName)
        } else {
          sparkSession.sql(sql)
        }
      }
      subType match {
        case "console" =>
          df.show()
        case "file" =>
          val path = cfg("path")
          val append = cfg("append").toBoolean
          val format = cfg.getOrDefault("format", ",")
          val out = new FileWriter(path, append)
          for (row <- df.collect()) {
            val line = {
              format match {
                case "json" =>
                  val jo = new JSONObject()
                  var i = 0
                  for (s <- row.schema) {
                    jo.put(s.name, row.get(i))
                    i += 1
                  }
                  jo.toJSONString
                case _ => row.mkString(format)
              }
            }
            out.write(line + "\n")
          }
          out.close()

        case "directory" =>
          val path = cfg("path")
          val append = cfg("append").toBoolean
          val mode = if (append) "append" else "overwrite"
          val dfWriter = df.write.mode(mode)
          val format = cfg.getOrDefault("format", "csv")
          format match {
            case "json" => dfWriter.json(path)
            case _ => dfWriter.csv(path)
          }

        case "db" =>
          val url = cfg("url")
          val dbtable = cfg("dbtable")
          val username = cfg("username")
          val password = cfg("password")
          val append = cfg("append").toBoolean
          val mode = if (append) "append" else "overwrite"
          val connectionProperties = new Properties()
          connectionProperties.put("user", username)
          connectionProperties.put("password", password)
          df.write.mode(mode).jdbc(url, dbtable, connectionProperties)

        case "hbase" =>
          val tableName = cfg("tableName")
          val family = cfg.getOrElse("family", "cf")

          df.foreachPartition(rowIterator => {
            val connection = ConnectionFactory.createConnection(KerberosUtil.createHbaseConfig())
            val table = connection.getTable(TableName.valueOf(tableName))
            while (rowIterator.hasNext) {
              val row = rowIterator.next()
              val put = {
                val put = new Put(row.get(0).toString.getBytes())
                var i = 0
                for (s <- row.schema) {
                  put.addColumn(family.getBytes(), s.name.getBytes(), row.get(i).toString.getBytes())
                  i += 1
                }
                put
              }
              table.put(put)
            }
            table.close()
            connection.close()
          })
        case _ =>

          df.show()
      }
    })
    null
  }
}
