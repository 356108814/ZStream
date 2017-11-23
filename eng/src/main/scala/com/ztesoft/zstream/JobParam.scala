package com.ztesoft.zstream

import java.io.{File, FileNotFoundException}

import com.alibaba.fastjson.JSON

import scala.io.Source

/**
  * 作业参数
  *
  * @author Yuri
  */
class JobParam(args: Array[String]) {
  val CONF_PATH = "confPath"
  val params = scala.collection.mutable.Map[String, String]()
  var jobConf: JobConf = _

  def parse() = {
    //解析命令行参数
    for (s <- args) {
      val a = s.split("=")
      val name = a(0).trim
      if (!name.equals("")) {
        params.put(name, a(1).trim)
      }
    }

    //解析任务配置json
    val jobConfPath = params(CONF_PATH)
    require(!jobConfPath.isEmpty, s"param $CONF_PATH must be set")

    val confFile = new File(jobConfPath)
    if (!confFile.exists() || !confFile.isFile) {
      throw new FileNotFoundException(s"配置文件${jobConfPath}不存在")
    }
    val confContent = Source.fromFile(jobConfPath, "utf-8").getLines().mkString("\n")
    jobConf = JSON.parseObject(confContent, classOf[JobConf])
    jobConf.assignParams()
  }

  parse()
}
