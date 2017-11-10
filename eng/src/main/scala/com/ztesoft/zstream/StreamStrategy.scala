package com.ztesoft.zstream

/**
  * 流策略，不同类型对应不同的执行引擎
  *
  * @author Yuri
  */
trait StreamStrategy {
  def start()

  def stop()
}
