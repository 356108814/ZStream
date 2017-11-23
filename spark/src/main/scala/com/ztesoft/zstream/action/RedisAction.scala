package com.ztesoft.zstream.action

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.sql._
import redis.clients.jedis.JedisPool

/**
  * 保存至redis操作
  *
  * @author Yuri
  */
class RedisAction(host: String, port: Int, dbIndex: Int, key: String = "", timeout: Int = 30000) extends Serializable {
  def process(df: DataFrame): Unit = {
    //必须加lazy延迟初始化，否则有序列化问题
    lazy val pool = new JedisPool(new GenericObjectPoolConfig(), host, port, timeout)

    df.foreachPartition(iteratorRow => {
      val redis = pool.getResource
      iteratorRow.foreach(row => {
        redis.select(dbIndex)
        val k = row.get(0).toString
        val v = row.get(row.size - 1).toString
        if (!key.equals("")) {
          redis.hincrBy(key, k, v.toLong)
        } else {
          redis.set(k, v)
        }
      })
      redis.close()
    })
  }
}
