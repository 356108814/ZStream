package com.ztesoft.zstream

/**
  * 流策略，不同类型对应不同的执行引擎
  * @author Yuri
  * @create 2017-11-7 16:27
  */
trait StreamStrategy {
  def start()
  def stop()
}
