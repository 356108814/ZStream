package com.ztesoft.zstream

/**
  * 引擎策略管理器
  *
  * @author Yuri
  */
class StrategyManager(jobConf: JobConf) {
  val strategyMap = Map[String, String](
    "spark" -> "com.ztesoft.zstream.pipeline.SparkStreamPipelineStrategy"
  )

  def start() = {
    val engineType = if(jobConf.getEngineType.nonEmpty) jobConf.getEngineType else "spark"
    val className = strategyMap(engineType)
    Class.forName(className)
      .getMethod("create", classOf[JobConf])
      .invoke(null, jobConf).asInstanceOf[StreamStrategy]
      .start()
  }
}
