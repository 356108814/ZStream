package com.ztesoft.zstream

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.io.Source

/**
  * 文件接收器，将文件按行转成流dstream
  *
  * @author Yuri
  */
class FileReceiver(filePath: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  override def onStart(): Unit = {
    new Thread("File Receiver") {
      override def run(): Unit = {
        receive()
      }
    }.start()
  }

  override def onStop(): Unit = {

  }

  private def receive() {
    try {
      val lines = Source.fromFile(filePath, "utf-8").getLines()
      while (!isStopped) {
        for (line <- lines) {
          store(line)
        }
      }
      restart("Trying to receive again")
    } catch {
      case t: Throwable =>
        restart("Error receiving data", t)
    }
  }
}
