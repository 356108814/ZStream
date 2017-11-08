package com.ztesoft.zstream

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark
  *
  * @author Yuri
  * @create 2017-11-7 17:10
  */
class SparkStreamStrategy(params: java.util.Map[String, String]) extends DefaultStreamStrategy with StreamStrategy {

  override def start(): Unit = {
    //    val appName = "main"
    //    val master = "local[4]"
    //    val filePath = "J:/spark/source/user.txt"
    //    val conf = new SparkConf().setAppName(appName).setMaster(master)
    //    val sc = new SparkContext(conf)
    //    val lines = sc.textFile(filePath)
    //    lines.collect().foreach(println)
    //    sc.stop()

    val conf = new SparkConf().setMaster("local[4]").setAppName("SocketTest")
    val ssc = new StreamingContext(conf, Seconds(5))
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
//    val lines = ssc.socketTextStream("localhost", 9999)
//    val dStream = lines.map(_.split(",")).map(a => Row(a(0).toInt, a(1)))
//
//    val relDF = sparkSession.read.json("file:///home/bdp/workspace/spark/run/rel.json")
//    relDF.createOrReplaceTempView("rel")
//
//    val result = dStream.transform(rdd => {
//      val structFields = List(StructField("id", IntegerType), StructField("name", StringType))
//      //最后通过StructField的集合来初始化表的模式。
//      val schema = StructType(structFields)
//      val df = sparkSession.createDataFrame(rdd, schema)
//      df.createOrReplaceTempView("user")
//      sparkSession.sql("select u.id, u.name, r.age from user u left join rel r on u.id = r.uid where id > 5").toJavaRDD
//    })

    val params = Map("sparkSession" -> sparkSession)
    val source = new SparkStreamSource()
    source.init(params)
    val result = source.process()

    result.take(0).asInstanceOf[DStream].foreachRDD(rdd => {
      rdd.foreach(row => println(row))
    })

    ssc.start()
    ssc.awaitTermination()
  }

  override def stop(): Unit = {

  }
}

object SparkStreamStrategy {
  def create(params: java.util.Map[String, String]): SparkStreamStrategy = {
    new SparkStreamStrategy(params)
  }
}
