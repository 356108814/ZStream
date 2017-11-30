package com.ztesoft.zstream.udf

import com.ztesoft.zstream.RedisUtil
import org.apache.spark.sql.api.java._
import redis.clients.jedis.JedisPool

/**
  * 按键自增累加函数(Double类型)
  *
  * @author Yuri
  */
class AccumulateDoubleUdf(param: Map[String, Any]) extends UDF3[String, String, Double, Double] {
  val host = param.getOrElse("host", "localhost").toString
  val port = param.getOrElse("port", "6379").toString.toInt
  lazy val pool: JedisPool = RedisUtil.getNewPool(host, port)

  override def call(key: String, field: String, value: Double): Double = {
    var accValue = 0.0
    lazy val redis = pool.getResource
    accValue = redis.hincrByFloat(key, field, value)
    redis.close()
    accValue
  }
}
