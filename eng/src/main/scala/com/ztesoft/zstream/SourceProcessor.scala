package com.ztesoft.zstream

/**
  * 数据源
  *
  * @author Yuri
  */
trait SourceProcessor[T] extends Processor[T] {

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

}
