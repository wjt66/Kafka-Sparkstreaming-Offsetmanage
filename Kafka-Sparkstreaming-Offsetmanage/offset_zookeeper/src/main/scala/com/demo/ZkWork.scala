package com.demo

/**
 *
 * @description: 使用Spark-Kafka-0-10版本整合,并手动提交偏移量,维护到Zookeeper中
 * @author: wanjintao
 * @time: 2020/6/27 14:56
 *
 */
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}

object ZkWork {
  val TIME_OUT = 5000
  var zooKeeper: ZooKeeper = _

  def watcher = new Watcher() {
    def process(event: WatchedEvent) {
      println(s"[ ZkWork ] process : " + event.getType)
    }
  }

  /** ***************************************************************************************************************
   * 基础方法
   * 连接zk,创建znode,更新znode
   */
  def connect() {
    println(s"[ ZkWork ] zk connect")
    zooKeeper = new ZooKeeper("10.103.104.181:2181,10.103.104.182:2181,10.103.104.183:2181", TIME_OUT, watcher)
  }

  def znodeCreate(znode: String, data: String) {
    println(s"[ ZkWork ] zk create /$znode , $data")
    zooKeeper.create(s"/$znode", data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
  }

  def znodeDataSet(znode: String, data: String) {
    println(s"[ ZkWork ] zk data set /$znode")
    zooKeeper.setData(s"/$znode", data.getBytes(), -1)
  }

  /** ***************************************************************************************************************
   * 工作方法
   * 获得znode数据
   * 判断znode是否存在
   * 更新znode数据
   */
  def znodeDataGet(znode: String): Array[String] = {
    connect()
    println(s"[ ZkWork ] zk data get /$znode")
    try {   //同步获取这个path对应的目录节点存储的数据，watch参数来标识是否使用上文提到的默认Watcher来进行注册,stat可以用来指定数据的版本等信息，同时还可以设置是否监控这个目录节点数据的状态
      new String(zooKeeper.getData(s"/$znode", true, null), "utf-8").split(",")
    } catch {
      case _: Exception => Array()   //如果try正常执行，则没有异常抛出；否则执行Array操作
    }
  }

  def znodeIsExists(znode: String): Boolean ={
    connect()
    println(s"[ ZkWork ] zk znode is exists /$znode")
    zooKeeper.exists(s"/$znode", true) match {
      case null => false
      case _ => true
    }
  }

  def offsetWork(znode: String, data: String) {
    connect()
    println(s"[ ZkWork ] offset work /$znode")
    zooKeeper.exists(s"/$znode", true) match {
      case null => znodeCreate(znode, data)
      case _ => znodeDataSet(znode, data)
    }
    println(s"[ ZkWork ] zk close★★★")
    zooKeeper.close()
  }


}
