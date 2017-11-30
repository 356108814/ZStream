package com.ztesoft.zstream.udf

import com.ztesoft.zstream.RedisUtil
import org.apache.spark.sql.api.java._
import redis.clients.jedis.JedisPool

/**
  * 按键自增累加函数(Long类型)
  *
  * @author Yuri
  */
//sql中传入redis参数形式
//class AccumulateLongUdf extends UDF6[String, String, Long, String, Int, Int, Long] {
//  var pool: JedisPool = null
//
//  override def call(key: String, field: String, value: Long, host: String, port: Int = 6379, db: Int = 0): Long = {
//    if (pool == null) {
//      pool = RedisUtil.getNewPool(host, port)
//    }
//    var accValue = 0L
//    lazy val redis = pool.getResource
//    accValue = redis.hincrBy(key, field, value)
//    redis.close()
//    accValue
//  }
//}

//注册函数时传入
class AccumulateLongUdf(param: Map[String, Any]) extends UDF3[String, String, Long, Long] {
  val host = param.getOrElse("host", "localhost").toString
  val port = param.getOrElse("port", "6379").toString.toInt
  lazy val pool: JedisPool = RedisUtil.getNewPool(host, port)

  override def call(key: String, field: String, value: Long): Long = {
    var accValue = 0L
    lazy val redis = pool.getResource
    accValue = redis.hincrBy(key, field, value)
    redis.close()
    accValue
  }
}
