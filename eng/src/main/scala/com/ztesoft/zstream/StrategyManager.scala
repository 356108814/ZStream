package com.ztesoft.zstream

/**
  * 引擎策略管理器
  *
  * @author Yuri
  * @create 2017-11-7 16:43
  */
class StrategyManager(jobConf: JobConf) {

  def start() = {
    val className = "com.ztesoft.zstream.SparkStreamStrategy"
    Class.forName(className)
      .getDeclaredMethod("create", classOf[java.util.Map[String, String]])
      .invoke(null, jobConf).asInstanceOf[StreamStrategy]
      .start()
  }
}
