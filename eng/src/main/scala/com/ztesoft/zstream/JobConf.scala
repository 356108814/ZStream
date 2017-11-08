package com.ztesoft.zstream

/**
  * 作业配置新
  * @author Yuri
  * @create 2017-11-7 14:48
  */
class JobConf(param: JobParam) {
  val jobParam = param

  def getEngineType: String = {
    param.get(ParamsName.engineType)
  }
}
