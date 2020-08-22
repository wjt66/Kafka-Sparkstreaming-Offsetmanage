package com.demo

import org.joda.time.DateTime

/**
 * 时间解析
 */
object TimeParse extends Serializable {

  def timeStamp2String(timeStamp: String, format: String): String = {
    val ts = timeStamp.toLong
    new DateTime(ts).toString(format)
  }

  def timeStamp2String(timeStamp: Long, format: String): String = {
    new DateTime(timeStamp).toString(format)
  }

  //  // 测试
  //  def main(args: Array[String]): Unit = {
  //    println(TimeParse.timeStamp2String(System.currentTimeMillis().toString(), "yyyy-MM-dd HH:mm:ss"))
  //  }
}
