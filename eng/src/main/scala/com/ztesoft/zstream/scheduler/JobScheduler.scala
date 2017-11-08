package com.ztesoft.zstream.scheduler

import com.ztesoft.zstream.{JobContext, StrategyManager, StreamStrategy}

/**
  * 作业调度
  *
  * @author Yuri
  * @create 2017-11-7 11:16
  */
class JobScheduler(jobContext: JobContext) {

  def start() = {
    val strategyManager = new StrategyManager(jobContext.jobConf)
    strategyManager.start()
  }

  def stop() = {

  }
}
