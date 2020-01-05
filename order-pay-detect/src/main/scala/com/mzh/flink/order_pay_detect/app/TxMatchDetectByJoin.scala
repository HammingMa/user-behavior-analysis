package com.mzh.flink.order_pay_detect.app

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, ProcessJoinFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


object TxMatchDetectByJoin {


  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sourceDS: DataStream[String] = env.readTextFile("./data/OrderLog.csv")
    //val sourceDS: DataStream[String] =env.socketTextStream("localhost",7777)

    val orderDS: DataStream[OrderEventInfo] = sourceDS.map(
      line => {
        val lineArray: Array[String] = line.split(",")
        val orderId: Long = lineArray(0).trim.toLong
        val eventType: String = lineArray(1).trim
        val txId: String = lineArray(2).trim
        val eventTime: Long = lineArray(3).trim.toLong

        OrderEventInfo(orderId, eventType, txId, eventTime)
      }
    ).filter(_.txId != null)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEventInfo](Time.seconds(0)) {
        override def extractTimestamp(t: OrderEventInfo): Long = {
          t.eventTime * 1000L
        }
      })

    val sourceDS2: DataStream[String] = env.readTextFile("./data/ReceiptLog.csv")

    val receiptDS: DataStream[ReceiptInfo] = sourceDS2.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        val txId: String = dataArray(0).trim
        val payChannel: String = dataArray(1).trim
        val eventTime: Long = dataArray(2).trim.toLong

        ReceiptInfo(txId, payChannel, eventTime)
      }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptInfo](Time.seconds(0)) {
      override def extractTimestamp(t: ReceiptInfo): Long = {
        t.eventTime * 1000L
      }
    })


    orderDS.keyBy(_.txId)
      .intervalJoin(receiptDS.keyBy(_.txId))
      .between(Time.seconds(-10), Time.seconds(10))
      .process(new TxPayMatchByJoin())
      .print()


    env.execute("TX Pay Match Job")

  }


}

class TxPayMatchByJoin extends ProcessJoinFunction[OrderEventInfo,ReceiptInfo,(OrderEventInfo,ReceiptInfo)]{
  override def processElement(in1: OrderEventInfo, in2: ReceiptInfo, context: ProcessJoinFunction[OrderEventInfo, ReceiptInfo, (OrderEventInfo, ReceiptInfo)]#Context, collector: Collector[(OrderEventInfo, ReceiptInfo)]): Unit = {
    collector.collect(in1,in2)
  }
}
