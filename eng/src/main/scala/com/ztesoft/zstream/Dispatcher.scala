package com.ztesoft.zstream

import java.util.concurrent.ConcurrentHashMap

/**
  * 事件分配器
  *
  * @author Yuri
  * @create 2017-11-7 13:32
  */
trait Listener {
  def process(eventData: EventData)
}

class EventData(input: Any) {
  val data = input
}

object Dispatcher {
  private val listeners: java.util.Map[String, Listener] = new ConcurrentHashMap[String, Listener]

  def addListener(eventName: String, listener: Listener): Unit = {
    listeners.put(eventName, listener)
  }

  def removeListener(eventName: String): Unit = {
    listeners.remove(eventName)
  }

  def clear() = {
    listeners.clear()
  }

  def dispatch(eventName: String, eventData: EventData): Unit = {
    val listener = listeners.get(eventName)
    if(listener == null) {
      Logging.logWarning(s"$eventName not add listener")
      return
    }
    listener.process(eventData)
  }
}
