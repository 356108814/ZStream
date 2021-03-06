package com.ztesoft.zstream

/**
  *
  * @author Yuri
  */
object TestDispatcher {
  def main(args: Array[String]) {
    Dispatcher.addListener("start", new Listener {
      override def process(eventData: EventData): Unit = {
        println(eventData.data)
      }
    })

    Dispatcher.dispatch("start", new EventData("this is data"))
  }
}
