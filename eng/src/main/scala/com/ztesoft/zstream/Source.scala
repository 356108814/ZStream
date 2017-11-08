package com.ztesoft.zstream

import java.util.{Map => JMap, List => JList}

/**
  * 数据源
  *
  * @author Yuri
  * @create 2017-11-8 13:59
  */
trait Source[T] {

  def init(params: Map[String, Any])

  /**
    * 获取列定义
    */
  def getDef: List[ColumnDef]

  /**
    * 数据转换，默认不处理
    *
    * @param line 数据行
    * @return 转换后的数据
    */
  def transform(line: String): String = {
    line
  }

  /**
    * 数据过滤，返回false的将丢弃
    *
    * @param line 数据行
    */
  def filter(line: String): Boolean = {
    true
  }

  /**
    * 数据源处理，返回处理后的结果集
    */
  def process(): List[T]
}
