package com.ztesoft.zstream.udf

import com.ztesoft.zstream.RedisUtil
import org.apache.spark.sql.api.java._

/**
  * 按键自增累加函数(Double类型)
  *
  * @author Yuri
  */
class AccumulateDoubleUdf extends UDF3[String, String, Double, Double] {
  override def call(t1: String, t2: String, t3: Double): Double = {
    val redis = RedisUtil.getJedis
    redis.hincrByFloat(t1, t2, t3)
  }
}
