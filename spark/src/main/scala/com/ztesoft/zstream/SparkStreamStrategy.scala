package com.ztesoft.zstream

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

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

    val params = Map("sparkSession" -> sparkSession, "ssc" -> ssc)
    val colDef = "[{\"id\": 1, \"name\": \"id\", \"type\": \"string\"}, {\"id\": 2, \"name\": \"name\", \"type\": \"string\"}]"
    val sources = List(Map("format" -> "socket", "path" -> "", "tableName" -> "user", "colDef" -> colDef, "host" -> "10.45.47.66", "port" -> 9999))
    val sparkSteamSource = new SparkStreamSource()
    sparkSteamSource.init(sources, params)
    val result = sparkSteamSource.process()
    val dStream = result.head.asInstanceOf[DStream[Row]]

    val ds = dStream.transform(rowRDD => {
      val r = sparkSession.sql("select * from user where id > 6")
      r.createOrReplaceTempView("adult")
      rowRDD.toJavaRDD()
    })

    ds.foreachRDD(rowRDD => {
      val adult = sparkSession.table("adult")
      adult.show()
    })

    ssc.start()
    ssc.awaitTermination()
  }

  override def stop(): Unit = {

  }
}

object SparkStreamStrategy {
  def main(args: Array[String]) {
    val params = new util.HashMap[String, String]()
    val strategy = create(params)
    strategy.start()
  }

  def create(params: java.util.Map[String, String]): SparkStreamStrategy = {
    new SparkStreamStrategy(params)
  }
}
