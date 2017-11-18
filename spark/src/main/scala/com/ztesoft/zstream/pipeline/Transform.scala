package com.ztesoft.zstream.pipeline

import com.ztesoft.zstream.{GlobalCache, JobConf, SparkUtil}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

import scala.collection.JavaConversions._

/**
  * 转换
  *
  * @author Yuri
  */
class Transform extends PipelineProcessor {
  /**
    * 数据转换处理
    *
    * @param input 输入数据
    * @return 处理后的结果集，键为输出表名
    */
  override def process(input: Option[DStream[Row]]): Option[DStream[Row]] = {
    val jobConf = GlobalCache.jobConf
    val cfg = conf.map(s => (s._1.toString, s._2.toString))
    val sql = cfg("sql")
    val acc = cfg.getOrElse("acc", "false").toBoolean
    val inputTableName = cfg("inputTableName")
    val outputTableName = cfg("outputTableName")
    val dstream = input.get

    val windowDuration = cfg.getOrElse("windowDuration", "0").toInt
    val slideDuration = cfg.getOrElse("slideDuration", "0").toInt

    val windowDStream = {
      if (windowDuration > 0 && slideDuration > 0) {
        dstream.window(Seconds(windowDuration), Seconds(slideDuration))
      } else if (windowDuration > 0) {
        dstream.window(Seconds(windowDuration))
      } else {
        dstream
      }
    }

    val sqlDStream = windowDStream.transform(rowRDD => {
      val sparkSession = SparkSession.builder().config(rowRDD.sparkContext.getConf).getOrCreate()
      val colDef = jobConf.getTableDef.get(inputTableName)
      val schema = SparkUtil.createSchema(colDef)
      val df = sparkSession.createDataFrame(rowRDD, schema)
      df.createOrReplaceTempView(inputTableName)
      //根据查询结果创建新表
      val returnDF = sparkSession.sql(sql)
      returnDF.createOrReplaceTempView(outputTableName)
      returnDF.toJavaRDD
    })

    val resultDStream = {
      if (acc) {
        val colDef = jobConf.getTableDef.get(outputTableName)
        val outputSchema = SparkUtil.createSchema(colDef)
        val accValueIndex = outputSchema.size - 1
        val accValueType = outputSchema.get(accValueIndex).dataType

        //累加row的最后一个值
        val accFunc = (newValues: Seq[Row], state: Option[Row]) => {
          val stateValue = {
            if (state.isDefined) {
              val row = state.get
              val value = {
                accValueType match {
                  case DoubleType => row.getDouble(accValueIndex)
                  case _ => row.getLong(accValueIndex)
                }
              }
              value
            } else {
              val value = accValueType match {
                case DoubleType => 0.0
                case _ => 0L
              }
              value
            }
          }
          val newValue = {
            var sum = {
              val value = accValueType match {
                case DoubleType => 0.0
                case _ => 0L
              }
              value
            }
            for (row <- newValues) {
              val value = {
                accValueType match {
                  case DoubleType => row.getDouble(accValueIndex)
                  case _ => row.getLong(accValueIndex)
                }
              }
              sum += value
            }
            sum
          }
          val result = newValue + stateValue
          //最后的值类型转换为输出表一致
          val keyRow = {
            if (newValues.nonEmpty) newValues.head else state.get
          }
          val row = {
            accValueType match {
              case IntegerType => Row(keyRow.get(0), result.toInt)
              case FloatType => Row(keyRow.get(0), result.toFloat)
              case DoubleType => Row(keyRow.get(0), result.toDouble)
              case _ => Row(keyRow.get(0), result.toLong)
            }
          }
          Some(row)
        }

        //第一个字段为键，最后一个字段为累加值
        val stateDStream = sqlDStream.map(row => (row(0), row)).updateStateByKey[Row](accFunc)
        //checkpoint保存
        val interval = jobConf.getParams.get("checkpointInterval", "60").toString.toInt
        stateDStream.cache()
        stateDStream.checkpoint(Seconds(interval))

        val resultDStream = stateDStream.map(t => t._2).transform(rowRDD => {
          val sparkSession = SparkSession.builder().config(rowRDD.sparkContext.getConf).getOrCreate()
          val colDef = jobConf.getTableDef.get(outputTableName)
          val schema = SparkUtil.createSchema(colDef)
          val df = sparkSession.createDataFrame(rowRDD, schema)
          df.createOrReplaceTempView(outputTableName)
          df.toJavaRDD
        })

        resultDStream

      } else {
        sqlDStream
      }
    }

    Option(resultDStream)
  }
}