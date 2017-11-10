package com.ztesoft.zstream

import com.alibaba.fastjson.JSON

import scala.beans.BeanProperty

class User() {
  @BeanProperty var id: String = _
  @BeanProperty var name: String = _

  override def toString: String = s"$id:$name"
}

/**
  *
  * @author Yuri
  * @create 2017-11-10 17:23
  */
object TestJson {

  def main(args: Array[String]) {
    val s = "{\"id\": 1, \"name\": \"yuri\"}"
    val user: User = JSON.parseObject(s, classOf[User])
    println(user)
  }
}
