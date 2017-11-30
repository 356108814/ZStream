package com.ztesoft.zstream

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * redis工具类
  *
  * @author Yuri
  */
object RedisUtil extends Serializable {
  var inited: Boolean = false
  var redisHost = "localhost"
  var redisPort = 6379
  var redisTimeout = 30000
  var pool: JedisPool = null

  lazy val hook = new Thread {
    override def run() = {
      if (pool != null) {
        pool.destroy()
      }
    }
  }
  sys.addShutdownHook(hook.run())

  def getJedis: Jedis = {
    if (!inited) {
      init()
      inited = true
    }
    pool.getResource
  }

  def getNewPool(host: String, port: Int, timeout: Int = 30000): JedisPool = {
    val config = new GenericObjectPoolConfig()
    new JedisPool(config, host, port, timeout)
  }

  private def init(): Unit = {
    redisHost = Config.properties.getProperty("redis.host")
    redisPort = Config.properties.getProperty("redis.port", "6379").toInt
    redisTimeout = Config.properties.getProperty("redis.timeout", "30000").toInt
    val config = new GenericObjectPoolConfig()
    pool = new JedisPool(config, redisHost, redisPort, redisTimeout)
  }
}
