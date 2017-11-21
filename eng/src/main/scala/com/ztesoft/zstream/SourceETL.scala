package com.ztesoft.zstream

import java.util

/**
  * 数据源ETL
  *
  * @author Yuri
  */
object SourceETL {

  def filterFile(filePath: String, className: String = "com.ztesoft.zstream.DefaultSourceExtProcessor"): Boolean = {
    val sourceExtProcessor = Class.forName(className).newInstance().asInstanceOf[SourceExtProcessor]
    sourceExtProcessor.filterFile(filePath)
  }

  def filterLine(line: String, format: String, columnDefs: util.List[ColumnDef], className: String = "com.ztesoft.zstream.DefaultSourceExtProcessor"): Boolean = {
    val sourceExtProcessor = Class.forName(className).newInstance().asInstanceOf[SourceExtProcessor]
    sourceExtProcessor.filterLine(line, format, columnDefs)
  }

  def transformLine(line: String, format: String, columnDefs: util.List[ColumnDef], className: String = "com.ztesoft.zstream.DefaultSourceExtProcessor"): String = {
    val sourceExtProcessor = Class.forName(className).newInstance().asInstanceOf[SourceExtProcessor]
    sourceExtProcessor.transformLine(line, format, columnDefs)
  }

}
