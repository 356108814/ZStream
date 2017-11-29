package com.ztesoft.zstream.udf

import com.ztesoft.zstream.RedisUtil
import org.apache.spark.sql.api.java._
import redis.clients.jedis.Jedis

/**
  * 按键自增累加函数(Double类型)
  *
  * @author Yuri
  */
class AccumulateDoubleUdf extends UDF3[String, String, Double, Double] {
  override def call(t1: String, t2: String, t3: Double): Double = {
    var value = 0.0
    var redis: Jedis = null
    try {
      redis = RedisUtil.getJedis
      value = redis.hincrByFloat(t1, t2, t3)
    } finally {
      if (redis != null) {
        redis.close()
      }
    }
    value
  }
}
