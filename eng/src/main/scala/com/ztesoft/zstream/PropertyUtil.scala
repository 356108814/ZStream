package com.ztesoft.zstream

import java.io.FileInputStream
import java.lang.reflect.{Field, Modifier}
import java.util.Properties

/**
  * 配置属性工具类
  *
  * @author Yuri
  */
object PropertyUtil {
  def getProperties(filename: String) = {
    val properties: Properties = new Properties
    val inputStream = {
      try {
        new FileInputStream(filename)
      } catch {
        case e: Any =>
          this.getClass.getClassLoader.getResourceAsStream(filename)
      }
    }
    properties.load(inputStream)
    properties
  }

  def findField(clazz: Class[_], name: String): Field = {
    val fields: Array[Field] = clazz.getDeclaredFields
    for (field <- fields) {
      if (name == field.getName) {
        if (Modifier.toString(field.getModifiers).indexOf("final") > 0) {
          return null
        }
        return field
      }
    }
    null
  }

  def reflectSetProperty(clazz: Class[_], name: String, value: String) {
    val field: Field = findField(clazz, name)
    if (field == null) {
      Logging.logWarning("属性[" + name + "]没有定义或者不可修改，忽略处理")
      return
    }
    val `type`: Class[_] = field.getType
    try {
      if (`type` eq classOf[String]) {
        field.set(clazz, value)
      }
      else if (`type` eq classOf[Boolean]) {
        field.setBoolean(clazz, value.toBoolean)
      }
      else if (`type` eq classOf[Int]) {
        field.setInt(clazz, value.toInt)
      }
      else if (`type` eq classOf[Long]) {
        field.setLong(clazz, value.toLong)
      }
    }
    catch {
      case e: Any =>
        Logging.logError(e.getMessage)
    }
  }
}
