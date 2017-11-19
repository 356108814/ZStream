package com.ztesoft.zstream.udf

import org.apache.spark.sql.api.java._

/**
  * 测试自定义函数
  *
  * @author Yuri
  */
class TestEncrypt extends UDF1[String, String] {
  override def call(t1: String): String = {
    s"007_$t1"
  }
}
