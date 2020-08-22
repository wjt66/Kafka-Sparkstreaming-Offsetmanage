package com.demo

/**
 *
 * @description: wordcount
 * @author: wanjintao
 * @time: 2020/6/23 12:11
 */

import java.lang
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
object Direct_WordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("direct_wordcount")
      .getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(3))
    //设置checkpoint
    //ssc.checkpoint("e:/data/Kafka_Receiver") westgis157:9092,west158:9092,
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "westgis181:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "68",
      "auto.offset.reset" -> "latest", //earliest,latest
      "enable.auto.commit" -> (false: lang.Boolean) //手动提交
    )

    val topic: Set[String] = Set("www212")

    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      // 订阅主题 注意需要给定消息的类型
      Subscribe[String, String](topic, kafkaParams)
    )

    val words: DStream[(String, Long)] = dstream.map(record => record.value).flatMap(_.split(" ")).map(x => (x, 1L))
    val word: DStream[(String, Long)] = words.reduceByKey(_ + _)
    word.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
