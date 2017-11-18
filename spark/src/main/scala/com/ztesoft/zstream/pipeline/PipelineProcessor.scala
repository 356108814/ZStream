package com.ztesoft.zstream.pipeline

import com.ztesoft.zstream.JobConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.Row

/**
  * 数据按pipeline形式处理
  *
  * @author Yuri
  */
trait PipelineProcessor {
  protected var conf: java.util.Map[String, Object] = _

  /**
    * 初始化
    *
    * @param conf 处理器配置
    */
  def init(conf: java.util.Map[String, Object]): Unit = {
    this.conf = conf
  }

  /**
    * 数据转换处理
    *
    * @param input 输入数据
    * @return 处理后的结果集，键为输出表名
    */
  def process(input: Option[DStream[Row]]): Option[DStream[Row]]
}

