package com.ztesoft.zstream

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._

/**
  * spark工具类
  *
  * @author Yuri
  */
object SparkUtil {

  def createSchema(jsonColDef: String): StructType = {
    val structFields = new util.ArrayList[StructField]()
    val colDef = JSON.parseArray(jsonColDef)
    val typeMap = Map(
      "byte" -> ByteType, "short" -> ShortType, "int" -> IntegerType, "long" -> LongType,
      "float" -> FloatType, "double" -> DoubleType,
      "date" -> DateType, "timestamp" -> TimestampType, "boolean" -> BooleanType,
      "string" -> StringType)

    for (col <- colDef.toArray) {
      val jo = col.asInstanceOf[com.alibaba.fastjson.JSONObject]
      val name = jo.getString("name")
      val cType = jo.getString("type")
      if (!typeMap.contains(cType)) {
        Logging.logWarning("不支持的数据类型：" + cType)
      }
      structFields.add(StructField(name, typeMap.getOrElse(cType, StringType), nullable = true))
    }
    StructType(structFields)
  }

  def createColumnDefList(jsonColDef: String): util.List[ColumnDef] = {
    val defList = new util.ArrayList[ColumnDef]()
    val colDef = JSON.parseArray(jsonColDef)

    for (col <- colDef.toArray) {
      val jo = col.asInstanceOf[com.alibaba.fastjson.JSONObject]
      val name = jo.getString("name")
      val cType = jo.getString("type")
      val cd = new ColumnDef()
      cd.setName(name)
      cd.setType(cType)
      defList.add(cd)
    }
    defList
  }

  /**
    * 字符串数组转spark Row
    *
    * @param array  数据
    * @param schema 表定义
    * @return Row
    */
  def arrayToRow(array: Array[String], schema: StructType): Row = {
    val values = new java.util.ArrayList[Any]()
    val structFields = schema.toArray
    for (i <- array.indices) {
      val v = array(i).toString
      val t = structFields.apply(i).dataType
      values.add(getValue(v, t))
    }
    Row.fromSeq(values.toArray)
  }

  /**
    * json对象转spark Row
    *
    * @param json   json数据
    * @param schema 表定义
    * @return Row
    */
  def jsonObjectToRow(json: JSONObject, schema: StructType): Row = {
    val values = new java.util.ArrayList[Any]()
    val structFields = schema.toArray
    for (field <- structFields) {
      val v = json.getString(field.name)
      val t = field.dataType
      values.add(getValue(v, t))
    }
    Row.fromSeq(values.toArray)
  }

  def getValue(value: String, valueType: DataType): Any = {
    valueType match {
      case ByteType => value.toByte
      case ShortType => value.toShort
      case IntegerType => value.toInt
      case LongType => value.toLong
      case FloatType => value.toFloat
      case DoubleType => value.toDouble
      case BooleanType => value.toBoolean
      case DateType => new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(value)
      case TimestampType => new Timestamp(java.lang.Long.parseLong(value))
      case _ => value
    }
  }

  /**
    * 当前action处理器配置是否为数据源源表对应的action
    *
    * @param sourceTableName 数据源表
    * @param conf            当前action processor配置
    * @param confList        全部processor配置
    * @return true符合条件
    */
  def isActionFromSource(sourceTableName: String, conf: java.util.Map[String, Object],
                         confList: java.util.List[java.util.Map[String, Object]]): Boolean = {

    def getPreConf(conf: java.util.Map[String, Object]): java.util.Map[String, Object] = {
      val inputTableName = conf.getOrElse("inputTableName", "").toString
      if (inputTableName.isEmpty) {
        return null
      }
      for (c <- confList) {
        val outputTableName = c.getOrElse("outputTableName", "")
        if (outputTableName.equals(inputTableName)) {
          return c
        }
      }
      null
    }

    var preConf = getPreConf(conf)
    while (preConf != null) {
      if (preConf.getOrElse("inputTableName", "").toString.isEmpty && preConf.get("outputTableName").toString.equals(sourceTableName)) {
        return true
      } else {
        preConf = getPreConf(preConf)
      }
    }
    false
  }

  def main(args: Array[String]) {
    val data = Array("1", "yuri")
    val s = "[{\"id\": 1, \"name\": \"a\", \"type\": \"int\"}, {\"id\": 2, \"name\": \"b\", \"type\": \"string\"}]"
    val schema = createSchema(s)
    val row = arrayToRow(data, schema)
    println(row)
  }
}
