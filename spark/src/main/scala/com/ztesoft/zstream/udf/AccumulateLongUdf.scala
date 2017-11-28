package com.ztesoft.zstream.udf

import com.ztesoft.zstream.RedisUtil
import org.apache.spark.sql.api.java._

/**
  * 按键自增累加函数(Long类型)
  *
  * @author Yuri
  */
class AccumulateLongUdf extends UDF3[String, String, Long, Long] {
  override def call(t1: String, t2: String, t3: Long): Long = {
    val redis = RedisUtil.getJedis
    redis.hincrBy(t1, t2, t3)
  }
}
