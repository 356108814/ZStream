package com.ztesoft.zstream

import java.io.FileInputStream

import org.python.core.{PyFunction, PyString}
import org.python.util.PythonInterpreter

/**
  *
  *
  * @author Yuri
  */
object TPython {
  def main(args: Array[String]): Unit = {
    val interpreter = new PythonInterpreter()
    interpreter.exec("print 111")

//    val func = interpreter.get("addNum", classOf[String]).asInstanceOf(PyFunction)
    val py_file = new FileInputStream("/Users/apple/debugData/udf.py")
    interpreter.execfile(py_file)
    py_file.close()
    val func = interpreter.get("uuidS")
    val result = func.__call__(new PyString("hello") )
    println(result)
    //        val pyobj = func.__call__(new PyInteger(a), new PyInteger(b))
  }

}
