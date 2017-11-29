package com.ztesoft.zstream

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * redis工具类
  *
  * @author Yuri
  */
object RedisUtil extends Serializable {
  var redisHost = Config.properties.getProperty("redis.host")
  var redisPort = Config.properties.getProperty("redis.port", "6379").toInt
  var redisTimeout = Config.properties.getProperty("redis.timeout", "30000").toInt
  val config = new GenericObjectPoolConfig()
  lazy val pool = new JedisPool(config, redisHost, redisPort, redisTimeout)

  lazy val hook = new Thread {
    override def run() = {
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run())

  def getJedis: Jedis = {
    pool.getResource
  }

  def getNewPool(host: String, port: Int, timeout: Int = 30000): JedisPool = {
    val config = new GenericObjectPoolConfig()
    new JedisPool(config, host, port, timeout)
  }
}
