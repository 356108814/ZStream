package com.ztesoft.zstream

import java.lang.reflect.Method

/**
  *
  * @author Yuri
  */
object T {
  def main(args: Array[String]) {
//    ("format" - "path" - "outputTable" - "data")
    val className = "com.ztesoft.zstream.udf.TestEncrypt"
    val clazz = Class.forName(className)
    val methods = clazz.getMethods
//    val callMethod: Method = methods.filter(method => method != clazz.getMethod("call", classOf[Object]) && method.getName.equals("call")).head
    val callMethod: Method = methods.filter(method => method.getName.equals("call") && method.getParameterCount > 0 && !method.getGenericParameterTypes.head.getTypeName.equals("java.lang.Object")).head
    val parameterCount = callMethod.getParameterCount
    val rtnType = callMethod.getGenericReturnType.getTypeName
    println(parameterCount)
    println(rtnType)
  }
}
