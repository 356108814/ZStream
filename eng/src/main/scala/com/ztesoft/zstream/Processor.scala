package com.ztesoft.zstream

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 处理器
  *
  * @author Yuri
  */
trait Processor[T] {
  /**
    * 初始化
    *
    * @param confList 处理器配置
    * @param params   全局参数
    */
  def init(confList: java.util.List[java.util.Map[String, Object]], params: scala.collection.mutable.Map[String, Any]): Unit

  /**
    * 数据转换处理
    *
    * @param input 输入数据
    * @return 处理后的结果集，键为输出表名
    */
  def process(input: java.util.List[T]): java.util.List[T]
}
