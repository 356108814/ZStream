package com.ztesoft.zstream

import java.util
import java.util.List

/**
  * 数据源ETL
  *
  * @author Yuri
  */
object SourceETL {

  def filter(line: String, format: String, columnDefs: util.List[ColumnDef], className: String = "com.ztesoft.zstream.DefaultSourceExtProcessor"): Boolean = {
    val sourceExtProcessor = Class.forName(className).newInstance().asInstanceOf[SourceExtProcessor]
    sourceExtProcessor.filter(line, format, columnDefs)
  }

  def transform(line: String, format: String, columnDefs: util.List[ColumnDef], className: String = "com.ztesoft.zstream.DefaultSourceExtProcessor"): String = {
    val sourceExtProcessor = Class.forName(className).newInstance().asInstanceOf[SourceExtProcessor]
    sourceExtProcessor.transform(line, format, columnDefs)
  }

}
