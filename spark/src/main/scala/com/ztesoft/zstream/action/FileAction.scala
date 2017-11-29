package com.ztesoft.zstream.action

import java.io.FileWriter

import com.alibaba.fastjson.JSONObject
import com.ztesoft.zstream.HdfsUtil
import org.apache.spark.sql._

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

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
    //先缓存所有文件以及对应需要写入的数据
    val fileMap = new collection.mutable.HashMap[String, ArrayBuffer[String]]()
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
        val content = {
          if (fileMap.contains(actualPath)) {
            fileMap.get(actualPath).get
          } else {
            val c = new ArrayBuffer[String]()
            fileMap.put(actualPath, c)
            c
          }
        }
        content += line
      } else {
        HdfsUtil.append(actualPath, line + "\n")
      }
    }
    for (t <- fileMap) {
      val fileWriter = new FileWriter(t._1, append)
      fileWriter.write(t._2.mkString("\n"))
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
    //提取参数${xxx}
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
