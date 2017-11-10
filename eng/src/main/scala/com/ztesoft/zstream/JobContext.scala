package com.ztesoft.zstream

import java.util.concurrent.atomic.AtomicReference

import com.ztesoft.zstream.scheduler.JobScheduler

/**
  * 作业唯一入口
  *
  * @author Yuri
  */
class JobContext(conf: JobConf) {
  val jobConf = conf

  private val jobScheduler: JobScheduler = new JobScheduler(this)
  private val activeContext = new AtomicReference[JobContext](null)

  def start() = {
    jobScheduler.start()
    setActiveContext(this)
  }

  def stop() = {
    jobScheduler.stop()
  }

  private def setActiveContext(context: JobContext): Unit = {
    activeContext.set(context)
  }
}
