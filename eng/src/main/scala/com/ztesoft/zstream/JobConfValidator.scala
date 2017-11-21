package com.ztesoft.zstream

import com.alibaba.fastjson.JSON

import scala.collection.JavaConversions._
import scala.io.Source

/**
  * 作业配置验证器
  *
  * @author Yuri
  */
class JobConfValidator(jobConf: JobConf) {

  def validate(): Unit = {
    validateJob()
    validateProcessor()
    validateTableDef()
    validateUdf()
    validateParam()
  }

  private def validateJob(): Unit = {
    requireAttr(jobConf.getId, "id")
    requireAttr(jobConf.getName, "name")
    requireAttr(jobConf.getEngineType, "engineType")
  }

  private def validateProcessor(): Unit = {
    var prefix = "processor"
    for (conf <- jobConf.getProcessors) {
      val pType = conf.getOrDefault("type", "").toString
      val pSubType = conf.getOrDefault("subType", "").toString
      requireAttr(pType, "type", prefix)
      requireAttr(pSubType, "subType", prefix)

      prefix = pType

      pType match {
        case "source" =>
          requireAttr(conf, "format", prefix)
          requireAttr(conf, "outputTableName", prefix)
          prefix = s"$prefix.$pSubType"
          pSubType match {
            case "socket" =>
              requireAttr(conf, "host", prefix)
              requireAttr(conf, "port", prefix)
            case "file" =>
              requireAttr(conf, "path", prefix)
            case "kafka" =>
              requireAttr(conf, "zkQuorum", prefix)
              requireAttr(conf, "group", prefix)
              requireAttr(conf, "topics", prefix)
            case "directory" =>
              requireAttr(conf, "path", prefix)
            case _ =>
              invalidAttr("subType", pSubType, prefix)
          }
        case "dim" =>
          requireAttr(conf, "outputTableName", prefix)
          pSubType match {
            case "file" =>
              requireAttr(conf, "path", prefix)
              requireAttr(conf, "format", prefix)
            case "jdbc" =>
              requireAttr(conf, "url", prefix)
              requireAttr(conf, "dbtable", prefix)
              requireAttr(conf, "username", prefix)
              requireAttr(conf, "password", prefix)
            case _ =>
              invalidAttr("subType", pSubType, prefix)
          }
        case "transform" =>
          pSubType match {
            case "sql" =>
              requireAttr(conf, "sql", prefix)
              requireAttr(conf, "inputTableName", prefix)
              requireAttr(conf, "outputTableName", prefix)
            case _ =>
              invalidAttr("subType", pSubType, prefix)
          }
        case "action" =>
          pSubType match {
            case "console" =>
            case "debug" =>
              requireAttr(conf, "url", prefix)
              requireAttr(conf, "dbtable", prefix)
            case "file" =>
              requireAttr(conf, "path", prefix)
              requireAttr(conf, "format", prefix)
            case "directory" =>
              requireAttr(conf, "path", prefix)
              requireAttr(conf, "format", prefix)
              val format = conf.get("format").toString
              if (!List("json", "csv").contains(format)) {
                invalidAttr("format", format, prefix, "可选值有：json、csv")
              }
            case "jdbc" =>
              requireAttr(conf, "url", prefix)
              requireAttr(conf, "dbtable", prefix)
              requireAttr(conf, "username", prefix)
              requireAttr(conf, "password", prefix)
            case "hbase" =>
              requireAttr(conf, "tableName", prefix)
              requireAttr(conf, "cf", prefix)
            case _ =>
              invalidAttr("subType", pSubType, prefix)
          }
        case _ =>
          invalidAttr("type", pType, "processor")
      }
    }
  }

  private def validateTableDef(): Unit = {
    val outputTableNames = jobConf.getOutputTableNames
    val tableDef = jobConf.getTableDef
    for (tableName <- outputTableNames) {
      if (tableDef == null || !tableDef.containsKey(tableName)) {
        throw new ZStreamException(ZStreamExceptionCode.JOB_CONF_ERROR, s"tableDef中，输出表${tableName}没配置定义")
      }
    }
  }

  private def validateUdf(): Unit = {

  }

  private def validateParam(): Unit = {
    val prefix = "param"
    val conf = jobConf.getParams
    jobConf.getEngineType match {
      case "spark" =>
        requireAttr(conf, "master", prefix)
        requireAttr(conf, "duration", prefix)
    }
  }

  def requireAttr(conf: java.util.Map[String, Object], name: String, prefix: String): Unit = {
    val attr = conf.getOrDefault(name, "").toString
    requireAttr(attr, name, prefix)
  }

  def requireAttr(attr: String, name: String, prefix: String = ""): Unit = {
    val isSet = attr != null && !attr.isEmpty
    if (!isSet) {
      throw new ZStreamException(ZStreamExceptionCode.JOB_CONF_ERROR, s"$prefix 缺少属性 $name")
    }
  }

  def invalidAttr(name: String, attr: String, prefix: String, suffix: String = ""): Unit = {
    throw new ZStreamException(ZStreamExceptionCode.JOB_CONF_ERROR, s"$prefix.$name:$attr 为无效的属性 $suffix")
  }
}

object Validator {
  def main(args: Array[String]) {
    val confPath = "G:\\GitHub\\ZStream\\spark\\src\\main\\resources\\first.json"
    val confContent = Source.fromFile(confPath, "utf-8").getLines().mkString("\n")
    val jobConf = JSON.parseObject(confContent, classOf[JobConf])
    val v = new JobConfValidator(jobConf)
    v.validate()
  }
}
