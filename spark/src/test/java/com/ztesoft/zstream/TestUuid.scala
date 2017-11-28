package com.ztesoft.zstream

import org.apache.spark.sql.api.java.UDF2

/**
  *
  *
  * @author Yuri
  */
class TestUuid extends UDF2[Int, String, String]{
  override def call(t1: Int, t2: String): String = {
    s"uuid_${t1.toString}_$t2"
  }
}
