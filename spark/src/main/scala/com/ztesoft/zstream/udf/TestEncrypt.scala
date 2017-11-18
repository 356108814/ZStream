package com.ztesoft.zstream.udf

import org.apache.spark.sql.api.java._

/**
  * 测试自定义函数
  *
  * @author Yuri
  */
class TestEncrypt extends UDF2[Int, Integer, String] {
  override def call(t1: Int, t2: Integer): String = {
    s"007_${t1.toString}_$t2"
  }
}
