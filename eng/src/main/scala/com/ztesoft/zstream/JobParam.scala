package com.ztesoft.zstream

import java.util

import com.ztesoft.zstream.ParamsName.ParamsName

/**
  * 参数工具类
  *
  * @author Yuri
  * @create 2017-11-7 14:53
  */
class JobParam(args: Array[String]) {
  val params: java.util.Map[String, String] = new util.HashMap[String, String]()

  def parse() = {
    for(s <- args) {
      val a = s.split("=")
      val name = a(0).trim
      if(!name.equals("")) {
        params.put(name, a(1).trim)
      }
    }
  }

  def get(name: ParamsName): String = {
    params.get(name.toString)
  }

  def isSet(name: ParamsName): Boolean = {
    val value = get(name)
    value != null && value.length > 0
  }

  /**
    * 参数验证
    */
  def validate() = {
    require(isSet(ParamsName.jobName), s"param ${ParamsName.jobName} must be set")
    require(isSet(ParamsName.engineType), s"param ${ParamsName.engineType} must be set")
    require(isSet(ParamsName.jobConfPath), s"param ${ParamsName.jobConfPath} must be set")
  }

  parse()
}

object ParamsName extends Enumeration{
  //这行是可选的，类型别名，在使用import语句的时候比较方便，建议加上
  type ParamsName = Value
  //作业参数定义，必选
  val jobName, engineType, jobConfPath = Value

  def main(args: Array[String]) {
    println(ParamsName.jobName)
  }
}
