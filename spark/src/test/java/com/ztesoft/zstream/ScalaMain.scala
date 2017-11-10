package com.ztesoft.zstream

import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark测试
  * @author Yuri
  * @create 2017-11-6 15:56
  */
object ScalaMain {
  def main(args: Array[String]) {
    val appName = "main"
    val master = "local[4]"
    var filePath = "J:/spark/source/user.txt"

    if(args.length != 0) {
      filePath = args(0)
    }

    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)
    val lines = sc.textFile(filePath)
    lines.collect().foreach(println)
    sc.stop()
  }
}
