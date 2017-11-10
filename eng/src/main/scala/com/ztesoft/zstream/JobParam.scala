package com.ztesoft.zstream

import com.alibaba.fastjson.JSON

import scala.io.Source
import com.ztesoft.zstream.ParamsName.ParamsName

/**
  * 作业参数
  *
  * @author Yuri
  */
class JobParam(args: Array[String]) {
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

    //解析任务配置json，TODO 判断配置文件是否存在
    require(params.get(ParamsName.confPath.toString).isDefined, s"param ${ParamsName.confPath} must be set")
    val jobConfPath = get(ParamsName.confPath)
    val confContent = Source.fromFile(jobConfPath, "utf-8").getLines().mkString("\n")
    jobConf = JSON.parseObject(confContent, classOf[JobConf])

    validate()
  }

  def get(name: ParamsName): String = {
    params.get(name.toString).get
  }

  def isSet(name: ParamsName): Boolean = {
    val value = get(name)
    value != null && value.length > 0
  }

  /**
    * 作业配置验证
    */
  def validate() = {
    //    jobConf
    //    require(isSet(ParamsName.jobName), s"param ${ParamsName.jobName} must be set")
    //    require(isSet(ParamsName.engineType), s"param ${ParamsName.engineType} must be set")
  }

  parse()
}

object ParamsName extends Enumeration {
  //这行是可选的，类型别名，在使用import语句的时候比较方便，建议加上
  type ParamsName = Value
  //作业参数定义，必选
  val jobName, engineType, confPath = Value

  def main(args: Array[String]) {
    println(ParamsName.jobName)
  }
}
