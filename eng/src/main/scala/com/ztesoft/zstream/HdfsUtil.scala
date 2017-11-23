package com.ztesoft.zstream

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

import scala.collection.mutable.ArrayBuffer

/**
  * hdfs操作工具类
  *
  * @author Yuri
  */
object HdfsUtil {
  /**
    * 读取文件
    *
    * @param filePath 文件路径
    * @param count    读取行数
    * @return 文件内容
    */
  def readFile(filePath: String, count: Int = -1): String = {
    val result = new ArrayBuffer[String]()
    var buffReader: BufferedReader = null
    try {
      val conf = KerberosUtil.createHdfsConfig()
      val fs = FileSystem.get(conf)
      buffReader = new BufferedReader(new InputStreamReader(fs.open(new Path(filePath))))
      var line = buffReader.readLine()
      var i = 0
      while (line != null && (count == -1 || i <= count)) {
        result += line
        line = buffReader.readLine()
        i += 1
      }
    } finally {
      if (buffReader != null) {
        buffReader.close()
      }
    }

    result.mkString("\n")
  }

  /**
    * 写入文件内容
    *
    * @param filePath 文件路径
    * @param content  内容
    */
  def writeFile(filePath: String, content: String): Unit = {
    var fos: FSDataOutputStream = null
    try {
      val conf = KerberosUtil.createHdfsConfig()
      val fs = FileSystem.get(conf)
      fos = fs.create(new Path(filePath), true)
      fos.writeChars(content)
    } finally {
      if (fos != null) {
        fos.close()
      }
    }
  }

  /**
    * 写入文件内容
    *
    * @param filePath 文件路径
    * @param content  内容
    */
  def append(filePath: String, content: String): Unit = {
    var fos: FSDataOutputStream = null
    try {
      val conf = KerberosUtil.createHdfsConfig()
//      conf.setBoolean("dfs.support.append", true)
      val fs = FileSystem.get(conf)
      fos = fs.append(new Path(filePath))
      fos.writeChars(content)
    } finally {
      if (fos != null) {
        fos.close()
      }
    }
  }

  def main(args: Array[String]) {
    System.setProperty("HADOOP_USER_NAME", "zdfs")
    val filepath = "/tmp/1/1.txt"
    HdfsUtil.writeFile(filepath, "aaa\nbbb")
    HdfsUtil.append(filepath, "ccc\nddd")
    println(HdfsUtil.readFile(filepath))
  }
}
