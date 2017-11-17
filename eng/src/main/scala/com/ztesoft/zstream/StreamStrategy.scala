package com.ztesoft.zstream

/**
  * 流策略，不同类型对应不同的执行引擎
  *
  * @author Yuri
  */
trait StreamStrategy {
  protected var conf: JobConf = _
  protected var params: java.util.Map[String, Object] = _

  def init(jobConf: JobConf) = {
    Logging.logInfo(jobConf.toString)
    this.conf = jobConf
    this.params = jobConf.getParams
  }

  def start()

  def stop()
}
