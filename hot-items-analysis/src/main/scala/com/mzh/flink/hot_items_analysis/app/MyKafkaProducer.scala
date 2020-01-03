package com.mzh.flink.hot_items_analysis.app

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.BufferedSource

object MyKafkaProducer {


  def main(args: Array[String]): Unit = {
    writeToKafka("UserBehavior")
  }

  def writeToKafka(topic: String): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hdp1:9092,hdp2:9092,hdp3:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val kafka = new KafkaProducer[String, String](properties)

    val source: BufferedSource = io.Source.fromFile("./data/UserBehavior.csv")

    for (line <- source.getLines()) {
     val record = new ProducerRecord[String, String](topic,line)
      kafka.send(record)
    }

    kafka.close()

  }
}
