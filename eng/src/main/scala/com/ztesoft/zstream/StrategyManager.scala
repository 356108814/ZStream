package com.ztesoft.zstream

/**
  * 引擎策略管理器
  *
  * @author Yuri
  */
class StrategyManager(jobConf: JobConf) {
  val strategyMap = Map(
    "spark" -> "com.ztesoft.zstream.SparkStreamStrategy"
  )

  def start() = {
    val className = strategyMap.getOrElse(jobConf.getEngineType, "spark")
    Class.forName(className)
      .getDeclaredMethod("create", classOf[java.util.Map[String, String]])
      .invoke(null, jobConf).asInstanceOf[StreamStrategy]
      .start()
  }
}
