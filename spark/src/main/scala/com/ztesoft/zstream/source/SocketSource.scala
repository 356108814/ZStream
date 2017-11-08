package com.ztesoft.zstream.source

import com.ztesoft.zstream.{ColumnDef, Source}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * socket数据源
  *
  * @author Yuri
  * @create 2017-11-8 14:12
  */
class SocketSource extends Source {
  override def process: Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val lines = ssc.socketTextStream("localhost", 9999)
    val dStream = lines.map(_.split(",")).map(a => Row(a(0).toInt, a(1)))

    val result = dStream.transform(rdd => {
      val structFields = List(StructField("id", IntegerType), StructField("name", StringType))
      //最后通过StructField的集合来初始化表的模式。
      val schema = StructType(structFields)
      val df = sparkSession.createDataFrame(rdd, schema)
      df.createOrReplaceTempView("user")
      sparkSession.sql("select * from user").toJavaRDD
      //      df.show()
      //      rdd.filter(f => true)
    })

    result.foreachRDD(rdd => {
      println(rdd)
    })

    ssc.start()
    ssc.awaitTermination()
  }

  override def init(params: Map[String, Any]): Unit = {}

  /**
    * 获取列定义
    */
  override def getDef: List[ColumnDef] = {
    List()
  }
}
