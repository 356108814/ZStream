package com.ztesoft.zstream.scheduler

import com.ztesoft.zstream.{JobContext, StrategyManager, StreamStrategy}

/**
  * 作业调度
  *
  * @author Yuri
  */
class JobScheduler(jobContext: JobContext) {

  def start() = {
    val strategyManager = new StrategyManager(jobContext.jobConf)
    strategyManager.start()
  }

  def stop() = {

  }
}
