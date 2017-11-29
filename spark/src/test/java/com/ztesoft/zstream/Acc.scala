package com.ztesoft.zstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * @author Yuri
  */
object Acc {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("acc").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("J:\\spark\\checkpoint")
    val lines = ssc.socketTextStream("10.45.47.66", 9999)
    val wordCount = lines.flatMap(_.split(",")).map(word => (word, 1L))
    val accFunc = (newValues: Seq[Long], runningCount: Option[Long]) => {
      val newCount = newValues.sum + runningCount.getOrElse(0L)
      Some(newCount)
    }
    wordCount.updateStateByKey[Long](accFunc).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
