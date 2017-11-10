package com.ztesoft.zstream

/**
  * 应用主入口
  *
  * @author Yuri
  * @create 2017-11-7 14:19
  */
object Application {
  def main(args: Array[String]) {
    for (s <- args) {
      println(s)
    }
    //参数设置和验证
    val param = new JobParam(args)
    val jobContext = new JobContext(param.jobConf)
    jobContext.start()
  }
}
