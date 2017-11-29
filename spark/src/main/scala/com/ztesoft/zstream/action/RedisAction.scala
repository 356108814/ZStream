package com.ztesoft.zstream.action

import com.ztesoft.zstream.RedisUtil
import org.apache.spark.sql._

/**
  * 保存至redis操作
  *
  * @author Yuri
  */
class RedisAction(host: String, port: Int, dbIndex: Int, key: String = "", timeout: Int = 30000) extends Serializable {
  def process(df: DataFrame): Unit = {
    lazy val pool = RedisUtil.getNewPool(host, port, timeout)
    val redis = pool.getResource

    df.foreachPartition(iteratorRow => {
      iteratorRow.foreach(row => {
        redis.select(dbIndex)
        val k = row.get(0).toString
        val v = row.get(row.size - 1).toString
        if (!key.isEmpty) {
          redis.hincrBy(key, k, v.toLong)
        } else {
          redis.set(k, v)
        }
      })
    })
    redis.close()
  }
}
