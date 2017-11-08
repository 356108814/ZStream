package com.ztesoft.zstream

/**
  * 应用主入口
  * @author Yuri
  * @create 2017-11-7 14:19
  */
object Application {
  def main(args: Array[String]) {
    for(s <- args) {
      println(s)
    }
    //参数设置和验证
    val param = new JobParam(args)
    param.validate()

    val conf = new JobConf(param)
    val jobContext = new JobContext(conf)
    jobContext.start()
  }
}
