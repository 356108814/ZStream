package com.ztesoft.zstream

import java.util.concurrent.ConcurrentHashMap

/**
  * 内存缓存
  *
  * @author Yuri
  */
object Cache {
  private val map: ConcurrentHashMap[String, Long] = new ConcurrentHashMap[String, Long]()

  def get(key: String): Long = {
    map.getOrDefault(key, 0)
  }

  def set(key: String, value: Long): Unit = {
    map.put(key, value)
  }

}
