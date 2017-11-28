package com.ztesoft.zstream

import java.io.FileInputStream
import java.lang.reflect.Method
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.api.java._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.python.core.PyString
import org.python.util.PythonInterpreter

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

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
      val cType = jo.getString("type").toLowerCase
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
    for (i <- schema.indices) {
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

  def getDefaultValue(valueType: DataType): Any = {
    valueType match {
      case ByteType => -1
      case ShortType => -1
      case IntegerType => -1
      case LongType => -1
      case FloatType => -1
      case DoubleType => -1
      case BooleanType => false
      case DateType => new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("1970-01-01 08:00:00")
      case TimestampType => new Timestamp(0)
      case _ => ""
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

  /**
    * 合并2行为1行
    *
    * @param row1        行1
    * @param row2        行2
    * @param schema1     表1定义
    * @param schema2     表2定义
    * @param queryColumn 需要查询的列列表，如果为空，则合并整个
    * @return
    */
  def mergeRow(row1: Option[Row], row2: Option[Row], schema1: StructType, schema2: StructType, queryColumn: ArrayBuffer[(Int, String)]): Row = {
    val values = new java.util.ArrayList[Any]()
    if (queryColumn.isEmpty) {
      //合并整个
      if (row1.isDefined) {
        for (v <- row1.get.toSeq) {
          values.add(v)
        }
      } else {
        for (s <- schema1) {
          values.add(getDefaultValue(s.dataType))
        }
      }

      if (row2.isDefined) {
        for (v <- row2.get.toSeq) {
          values.add(v)
        }
      } else {
        for (s <- schema2) {
          values.add(getDefaultValue(s.dataType))
        }
      }
    }
    else {
      for (t <- queryColumn) {
        val index = t._1
        val name = t._2
        if (index == 1) {
          if (row1.isDefined) {
            val row = row1.get
            values.add(row.get(row.fieldIndex(name)))
          } else {
            values.add(getDefaultValue(schema1.get(schema1.fieldIndex(name)).dataType))
          }
        } else {
          if (row2.isDefined) {
            val row = row2.get
            print()
            values.add(row.get(row.fieldIndex(name)))
          } else {
            values.add(getDefaultValue(schema2.get(schema2.fieldIndex(name)).dataType))
          }
        }
      }
    }
    Row.fromSeq(values.toArray)
  }

  /**
    * 注册自定义函数，最多支持8个参数
    *
    * @param sparkSession SparkSession
    * @param udfs         函数，键为函数名，值为类名
    */
  def registerUDF(sparkSession: SparkSession, udfs: java.util.Map[String, String]): Unit = {
    var udfMap: java.util.Map[String, String] = null
    if (udfs == null) {
      udfMap = new util.HashMap[String, String]()
    } else {
      udfMap = udfs
    }
    //默认自定义函数
    val defaultPackage = "com.ztesoft.zstream.udf."
    val defaultUdfs = Map[String, String](
      "accLong" -> "AccumulateLongUdf",
      "accDouble" -> "AccumulateDoubleUdf"
    )
    for (t <- defaultUdfs) {
      udfMap.put(t._1, defaultPackage + t._2)
    }

    udfMap.keys.foreach(name => {
      val className = udfMap.get(name)
      val clazz = Class.forName(className)
      val methods = clazz.getMethods
      val callMethod: Method = methods.filter(
        method => method.getName.equals("call")
          && method.getParameterCount > 0
          && !method.getGenericParameterTypes.head.getTypeName.equals("java.lang.Object")).head

      val parameterCount = callMethod.getParameterCount
      val rtnTypeName = callMethod.getGenericReturnType.getTypeName
      //兼容java和spark类型
      val typeMap = Map(
        "java.lang.Byte" -> ByteType, "java.lang.Short" -> ShortType, "java.lang.Integer" -> IntegerType, "java.lang.Long" -> LongType,
        "java.lang.Float" -> FloatType, "java.lang.Double" -> DoubleType,
        "java.util.Date" -> DateType, "java.sql.timestamp" -> TimestampType, "java.lang.Boolean" -> BooleanType,
        "java.lang.String" -> StringType, "byte" -> ByteType, "short" -> ShortType, "int" -> IntegerType, "long" -> LongType,
        "float" -> FloatType, "double" -> DoubleType,
        "date" -> DateType, "timestamp" -> TimestampType, "boolean" -> BooleanType,
        "string" -> StringType)
      val rtnType = typeMap(rtnTypeName)

      parameterCount match {
        case 1 =>
          sparkSession.udf.register(name, clazz.newInstance().asInstanceOf[UDF1[_, _]], rtnType)
        case 2 =>
          sparkSession.udf.register(name, clazz.newInstance().asInstanceOf[UDF2[_, _, _]], rtnType)
        case 3 =>
          sparkSession.udf.register(name, clazz.newInstance().asInstanceOf[UDF3[_, _, _, _]], rtnType)
        case 4 =>
          sparkSession.udf.register(name, clazz.newInstance().asInstanceOf[UDF4[_, _, _, _, _]], rtnType)
        case 5 =>
          sparkSession.udf.register(name, clazz.newInstance().asInstanceOf[UDF5[_, _, _, _, _, _]], rtnType)
        case 6 =>
          sparkSession.udf.register(name, clazz.newInstance().asInstanceOf[UDF6[_, _, _, _, _, _, _]], rtnType)
        case 7 =>
          sparkSession.udf.register(name, clazz.newInstance().asInstanceOf[UDF7[_, _, _, _, _, _, _, _]], rtnType)
        case 8 =>
          sparkSession.udf.register(name, clazz.newInstance().asInstanceOf[UDF8[_, _, _, _, _, _, _, _, _]], rtnType)
      }
    })
  }

  /**
    * 注册python实现的自定义函数
    *
    * @param sparkSession session
    * @param funcName     函数名称
    * @param pyFilePath   py文件路径
    */
  def registerPythonUdf(sparkSession: SparkSession, funcName: String, pyFilePath: String): Unit = {
    //TODO 测试python自定义函数
    sparkSession.udf.register("uuid", (input: String) => {
      val interpreter = new PythonInterpreter()
      val py_file = new FileInputStream("/Users/apple/debugData/udf.py")
      interpreter.execfile(py_file)
      py_file.close()
      val func = interpreter.get("uuidS")
      val result = func.__call__(new PyString(input))
      result.toString
    })
  }

  /**
    * row列表转换为map列表
    *
    * @param rows Row列表
    * @return List<Map>
    */
  def listRowToListMap(rows: List[Row]): java.util.List[java.util.Map[String, Object]] = {
    val list = new util.ArrayList[java.util.Map[String, Object]]()
    for (row <- rows) {
      val map: java.util.HashMap[String, Object] = new java.util.HashMap[String, Object]()
      for (s <- row.schema.toList) {
        map.put(s.name, row.get(row.fieldIndex(s.name)).asInstanceOf[Object])
      }
      list.add(map)
    }
    list
  }


  def main(args: Array[String]) {
    val data = Array("1", "yuri")
    val s = "[{\"id\": 1, \"name\": \"a\", \"type\": \"int\"}, {\"id\": 2, \"name\": \"b\", \"type\": \"string\"}]"
    val schema = createSchema(s)
    val row = arrayToRow(data, schema)
    println(row)
  }
}
