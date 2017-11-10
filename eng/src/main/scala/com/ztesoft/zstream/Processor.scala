package com.ztesoft.zstream

/**
  * 处理器
  *
  * @author Yuri
  * @create 2017-11-10 15:34
  */
trait Processor[T] {
  /**
    * 初始化
    *
    * @param confList 处理器配置
    * @param params   全局参数
    */
  def init(confList: List[Map[String, Any]], params: scala.collection.mutable.Map[String, Any])

  /**
    * 数据转换处理
    *
    * @param input 输入数据
    * @return 处理后的结果集，键为输出表名
    */
  def process(input: List[(String, T)]): List[(String, T)]
}
