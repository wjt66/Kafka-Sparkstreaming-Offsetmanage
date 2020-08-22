package com.demo

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext

import scala.reflect.ClassTag

/**
 *
 * @description:
 * @author: wanjintao
 * @time: 2020/8/18 16:38
 *
 */
// This wrapper lets us update brodcast variables within DStreams' foreachRDD
// without running into serialization issues
case class BroadcastWrapper[T: ClassTag](
                                          @transient private val ssc: StreamingContext,
                                          @transient private val _v: T) {

  @transient private var v = ssc.sparkContext.broadcast(_v)

  /** 更新广播变量
   *  @param newValue: T 新的待广播数据
   *  @param blocking: Boolean 是否阻塞广播变量的使用，直到广播变量重新广播完成
   */
  def update(newValue: T, blocking: Boolean = false): Unit = {
    // 删除RDD是否需要锁定
    v.unpersist(blocking)
    v = ssc.sparkContext.broadcast(newValue)
  }

  // 广播变量的数据
  def value: T = v.value

  // 序列化广播变量对象
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(v)
  }

  // 反序列化广播变量对象
  private def readObject(in: ObjectInputStream): Unit = {
    v = in.readObject().asInstanceOf[Broadcast[T]]
  }

}
