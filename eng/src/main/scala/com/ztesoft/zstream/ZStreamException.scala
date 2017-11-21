package com.ztesoft.zstream

import com.ztesoft.zstream.ZStreamExceptionCode.ZStreamExceptionCode

/**
  * 统一异常
  *
  * @author Yuri
  */
class ZStreamException(code: ZStreamExceptionCode, message: String)
  extends Exception(code.toString + " " + message) {

  def this(code: ZStreamExceptionCode) = this(code, "")
}


object ZStreamExceptionCode extends Enumeration {
  type ZStreamExceptionCode = Value
  val JOB_CONF_ERROR = Value
}
