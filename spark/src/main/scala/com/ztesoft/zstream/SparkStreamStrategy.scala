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
    val colDef1 = "[{\"name\": \"id\", \"type\": \"int\"}, {\"name\": \"name\", \"type\": \"string\"}]"
    val colDef2 = "[{\"name\": \"id\", \"type\": \"int\"}, {\"name\": \"age\", \"type\": \"int\"}]"
    val s1 = Map("sourceType" -> "socket", "path" -> "", "tableName" -> "user", "colDef" -> colDef1, "host" -> "localhost", "port" -> 9999)
    val s2 = Map("sourceType" -> "file", "path" -> "/Users/apple/debugData/user_rel.txt", "tableName" -> "user_rel", "colDef" -> colDef2, "format" -> "json")
    val s3 = Map("sourceType" -> "socket", "path" -> "", "tableName" -> "user_rel", "colDef" -> colDef2, "host" -> "localhost", "port" -> 9998)

    val sources = List(s1, s3)
    val sparkSteamSource = new SparkStreamSource()
    sparkSteamSource.init(sources, params)
    val result = sparkSteamSource.process()
    val dStream = result.apply(1).asInstanceOf[DStream[Row]]

//    val ds = dStream.transform(rowRDD => {
//      val sql = "select u.id, u.name, rel.age from user u, user_rel rel where u.id = rel.id and rel.age > 3"
//      val r = sparkSession.sql(sql)
//      r.createOrReplaceTempView("adult")
//      rowRDD.toJavaRDD()
//    })

        val ds = dStream.transform(rowRDD => {
          val sql = "select * from user_rel where age > 3"
          val r = sparkSession.sql(sql)
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
