package com.ztesoft.zstream

/**
  * 应用主入口
  *
  * @author Yuri
  */
object Application {
  def main(args: Array[String]) {
    for (s <- args) {
      println(s)
    }
    //参数设置和验证
    val param = new JobParam(args)
    val jobConfValidator = new JobConfValidator(param.jobConf)
    jobConfValidator.validate()
    Config.load("config.properties")
    val jobContext = new JobContext(param.jobConf)
    jobContext.start()
  }
}
