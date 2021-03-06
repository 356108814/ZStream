package com.ztesoft.zstream

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.collection.Iterator
import scala.io.Source

/**
  * 文件接收器，将文件按行转成流dstream
  *
  * @author Yuri
  */
class FileReceiver(filePath: String, encoding: String = "utf-8") extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

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
      var lines: Iterator[String] = null
      val schema = "file://"
      if (filePath.startsWith(schema)) {
        val realFilePath = filePath.replace(schema, "")
        lines = Source.fromFile(realFilePath, encoding).getLines()
      } else {
        lines = HdfsUtil.readFile(filePath).split("\n").toIterator
      }
      while (!isStopped && lines.nonEmpty) {
        for (line <- lines) {
          store(line)
        }
      }
    } catch {
      case t: Throwable =>
        restart("Error receiving data", t)
    }
  }
}
