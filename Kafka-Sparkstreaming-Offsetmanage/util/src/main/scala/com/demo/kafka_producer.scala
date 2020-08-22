package com.demo

/**
 *
 * @description:
 * @author: wanjintao
 * @time: 2020/6/23 15:51
 *
 */
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.{SparkConf, SparkContext}
object kafka_producer extends Serializable {   //Scala的object和class需要实现接口 Serializable 来序列化

  def main(args: Array[String]) {
    if (args.length < 6) {
      System.err.println("Usage: KafkaProducer <brokers> <topic> " +
        "<inputfile> <interval> <number> <producers>")
      System.exit(1)
    }

    val Array(brokers, topic, inputfile, interval, number, producers) = args

    val conf = new SparkConf().setAppName("kafka_producer").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val textfile = sc.textFile(inputfile).repartition(producers.toInt)

    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("acks", "1")
    props.put("retries", "3")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)

    textfile.foreachPartition { rows =>
      val producer = new KafkaProducer[String, String](props)
      var count = 0
      var index = 0
      while(rows.hasNext) {
        if (count == number.toInt) {

          Thread.sleep(interval.toInt)
          count = 0
        }
        count = count + 1
        index = index + 1
        val str = rows.next()
        println("发送的第"+ index +"条消息为: "+str)
        val message = new ProducerRecord[String, String](topic, null, str.toString)
        producer.send(message)
      }

    }
  }
}

