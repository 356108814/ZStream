package com.ztesoft.zstream

import java.util

import org.apache.hadoop.fs.Path

/**
  * 数据源ETL
  *
  * @author Yuri
  */
object SourceETL {

  def filterFile(path: Path, regex: String, className: String = "com.ztesoft.zstream.DefaultSourceExtProcessor"): Boolean = {
    //文件完整路径，如：/tmp/test.log
    val filePath = path.toUri.getPath
    val sourceExtProcessor = Class.forName(className).newInstance().asInstanceOf[SourceExtProcessor]
    val first = sourceExtProcessor.filterFile(filePath)
    var second = true
    //必须满足正则表达式
    if (regex != null && !regex.isEmpty) {
      second = filePath.matches(regex)
    }
    first && second
  }

  def filterLine(line: String, format: String, columnDefs: util.List[ColumnDef], className: String = "com.ztesoft.zstream.DefaultSourceExtProcessor"): Boolean = {
    val sourceExtProcessor = Class.forName(className).newInstance().asInstanceOf[SourceExtProcessor]
    val filter = sourceExtProcessor.filterLine(line, format, columnDefs)
    if (!filter) {
      Logging.logWarning(s"数据源丢弃行数据：$line")
    }
    filter
  }

  def transformLine(line: String, format: String, columnDefs: util.List[ColumnDef], className: String = "com.ztesoft.zstream.DefaultSourceExtProcessor"): String = {
    val sourceExtProcessor = Class.forName(className).newInstance().asInstanceOf[SourceExtProcessor]
    sourceExtProcessor.transformLine(line, format, columnDefs)
  }

}
