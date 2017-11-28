package com.ztesoft.zstream.action

import java.io.FileWriter

import com.alibaba.fastjson.JSONObject
import com.ztesoft.zstream.HdfsUtil
import org.apache.spark.sql._

import scala.collection.JavaConversions._

/**
  * 保存至文件
  *
  * @author Yuri
  */
class FileAction {
  def process(cfg: java.util.Map[String, String], df: DataFrame): Unit = {
    val path = cfg("path")
    val append = cfg.getOrElse("append", "true").toBoolean
    val format = cfg.getOrElse("format", ",")
    var fileWriter: FileWriter = null
    for (row <- df.collect()) {
      var actualPath = getActualPath(path, row)
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
      val schema = "file://"
      if (actualPath.startsWith(schema)) {
        actualPath = actualPath.replace(schema, "")
        fileWriter = new FileWriter(actualPath, append)
        fileWriter.write(line + "\n")
      } else {
        HdfsUtil.append(actualPath, line + "\n")
      }
    }
    if (fileWriter != null) {
      fileWriter.close()
    }
  }

  /**
    * 获取实际保存路径，支持参数替换
    *
    * @param path 路径
    * @param row  行值
    * @return 替换参数后的实际路径
    */
  private def getActualPath(path: String, row: Row): String = {
    var actualPath = path
    //提前参数${xxx}
    val pattern = "\\$\\{(\\s*\\w+\\.?\\w+\\s*)}".r
    val matchParams = pattern findAllMatchIn path
    for (m <- matchParams) {
      val paramName = m.group(1).trim
      val value = try {
        row.get(row.fieldIndex(paramName))
      } catch {
        case _: Throwable => null
      }
      if (value != null) {
        actualPath = actualPath.replaceAll("\\$\\{" + paramName + "}", value.toString)
      }
    }
    actualPath
  }
}
