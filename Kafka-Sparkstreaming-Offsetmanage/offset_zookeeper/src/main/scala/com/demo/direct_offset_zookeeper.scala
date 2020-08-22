package com.demo

/**
 *
 * @description: 使用Spark-Kafka-0-10版本整合,并手动提交偏移量,维护到Zookeeper中
 * @author: wanjintao
 * @time: 2020/6/27 13:48
 *
 */
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}


//手动控制spark 消费 kafka的偏移度
//保证spark在意外退出时，重启程序数据不丢失

object direct_offset_zookeeper {

  Logger.getLogger("org").setLevel(Level.WARN)

  //zookeeper 实例化，方便后面对zk的操作
  val zk = ZkWork

  def main(args: Array[String]): Unit = {

    val Array(brokers, topic, group, sec) = args

    val conf = new SparkConf().setAppName("direct_offset_zookeeper").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(sec.toInt))

    val topics = Array(topic)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "earliest",    //"latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
      //你可以通过增大会话时间（max.poll.interval.ms)
      // 或者减小poll()方法处理的最大记录条数（max.poll.records)
      //"max.poll.interval.ms" -> "KAFKA_MAX_POLL_INTERVAL_MS",
      //"max.poll.records" -> "KAFKA_MAX_POLL_RECORDS"
    )


    //    判断zk中是否有保存过该计算的偏移量
    //    如果没有保存过,使用不带偏移量的计算,在计算完后保存
    //    精髓就在于KafkaUtils.createDirectStream这个地方
    //    默认是KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))，不加偏移度参数
    //    实在找不到办法，最后啃了下源码。发现可以使用偏移度参数


    val stream = if (zk.znodeIsExists(s"${topic}offset")) {
      val nor = zk.znodeDataGet(s"${topic}offset")
      val newOffset = Map(new TopicPartition(nor(0).toString, nor(1).toInt) -> nor(2).toLong)//创建以topic，分区为k 偏移度为v的map

      println(s"[ DealFlowBills2 ] --------------------------------------------------------------------")
      println(s"[ DealFlowBills2 ] topic ${nor(0).toString}")
      println(s"[ DealFlowBills2 ] Partition ${nor(1).toInt}")
      println(s"[ DealFlowBills2 ] offset ${nor(2).toLong}")
      println(s"[ DealFlowBills2 ] zk中取出来的kafka偏移量★★★ $newOffset")
      println(s"[ DealFlowBills2 ] --------------------------------------------------------------------")
      KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, newOffset))
    } else {
      println(s"[ DealFlowBills2 ] --------------------------------------------------------------------")
      println(s"[ DealFlowBills2 ] 第一次计算,没有zk偏移量文件")
      println(s"[ DealFlowBills2 ] 手动创建一个偏移量文件 ${topic}offset 默认从0号分区 0偏移度开始计算")
      println(s"[ DealFlowBills2 ] --------------------------------------------------------------------")
      zk.znodeCreate(s"${topic}offset", s"$topic,0,0")
      val nor = zk.znodeDataGet(s"${topic}offset")
      val newOffset = Map(new TopicPartition(nor(0).toString, nor(1).toInt) -> nor(2).toLong)
      KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, newOffset))
    }


    //业务处理代码
    val lines = stream.map(_.value)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    //保存偏移度部分
    //如果在计算的时候失败了，会接着上一次偏移度进行重算，不保存新的偏移度
    //计算成功后保存偏移度

    stream.foreachRDD {
      rdd =>
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition {
          iter =>
            val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
            println(s"[ DealFlowBills2 ] --------------------------------------------------------------------")
            println(s"[ DealFlowBills2 ]  topic: ${o.topic}")
            println(s"[ DealFlowBills2 ]  partition: ${o.partition} ")
            println(s"[ DealFlowBills2 ]  fromOffset 开始偏移量: ${o.fromOffset} ")
            println(s"[ DealFlowBills2 ]  untilOffset 结束偏移量: ${o.untilOffset} 需要保存的偏移量,供下次读取使用★★★")
            println(s"[ DealFlowBills2 ] --------------------------------------------------------------------")
            // 写zookeeper
            zk.offsetWork(s"${o.topic}offset", s"${o.topic},${o.partition},${o.untilOffset}")

          // 写本地文件系统
          // val fw = new FileWriter(new File("/home/hadoop1/testjar/test.log"), true)
          // fw.write(offsetsRangerStr)
          // fw.close()
        }
    }

    //最后结果保存到hdfs
    //result.saveAsTextFiles(output + s"/output/" + "010")
    //spark streaming 开始工作
    ssc.start()
    ssc.awaitTermination()

  }
}

