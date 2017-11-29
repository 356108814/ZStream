package com.ztesoft.zstream.udf

import com.ztesoft.zstream.RedisUtil
import org.apache.spark.sql.api.java._
import redis.clients.jedis.Jedis

/**
  * 按键自增累加函数(Long类型)
  *
  * @author Yuri
  */
class AccumulateLongUdf extends UDF3[String, String, Long, Long] {
  override def call(t1: String, t2: String, t3: Long): Long = {
    var value = 0L
    var redis: Jedis = null
    try {
      redis = RedisUtil.getJedis
      value = redis.hincrBy(t1, t2, t3)
    } finally {
      if (redis != null) {
        redis.close()
      }
    }
    value
  }
}
