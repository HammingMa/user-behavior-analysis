package com.mzh.flink.order_pay_detect.app

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


case class OrderEventInfo(orderId: Long, eventType: String, txId: String, eventTime: Long)

case class ReceiptInfo(txId: String, payChannel: String, eventTime: Long)

object TxMacthDetect {

  private val unMatchReceipt = new OutputTag[OrderEventInfo]("unMatchReceipt")
  private val unMatchPay = new OutputTag[ReceiptInfo]("unMatchPay")

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


    val connS: ConnectedStreams[OrderEventInfo, ReceiptInfo] = orderDS.keyBy(_.txId).connect(receiptDS.keyBy(_.txId))

    val resultDS: DataStream[(OrderEventInfo, ReceiptInfo)] = connS.process(new TxPayMatch())

    resultDS.print("match")
    resultDS.getSideOutput[OrderEventInfo](unMatchReceipt).print("unMatchReceipt")
    resultDS.getSideOutput[ReceiptInfo](unMatchPay).print("unMatchPay")

    env.execute("TX Pay Match Job")

  }

  class TxPayMatch extends CoProcessFunction[OrderEventInfo, ReceiptInfo, (OrderEventInfo, ReceiptInfo)] {

    private lazy val payState: ValueState[OrderEventInfo] = getRuntimeContext.getState[OrderEventInfo](new ValueStateDescriptor[OrderEventInfo]("pay-stat", classOf[OrderEventInfo]))
    private lazy val receiptState: ValueState[ReceiptInfo] = getRuntimeContext.getState[ReceiptInfo](new ValueStateDescriptor[ReceiptInfo]("receipt-stat", classOf[ReceiptInfo]))

    override def processElement1(pay: OrderEventInfo, context: CoProcessFunction[OrderEventInfo, ReceiptInfo, (OrderEventInfo, ReceiptInfo)]#Context, collector: Collector[(OrderEventInfo, ReceiptInfo)]): Unit = {
      val receiptInfo: ReceiptInfo = receiptState.value()

      if (receiptInfo != null) {
        collector.collect((pay, receiptInfo))
        receiptState.clear()
      } else {
        payState.update(pay)

        context.timerService().registerEventTimeTimer(pay.eventTime * 1000L + 5000L)
      }

    }

    override def processElement2(receipt: ReceiptInfo, context: CoProcessFunction[OrderEventInfo, ReceiptInfo, (OrderEventInfo, ReceiptInfo)]#Context, collector: Collector[(OrderEventInfo, ReceiptInfo)]): Unit = {
      val pay: OrderEventInfo = payState.value()
      if (pay != null) {
        collector.collect((pay, receipt))
        payState.clear()
      } else {
        receiptState.update(receipt)
        context.timerService().registerEventTimeTimer(receipt.eventTime * 1000L + 5000L)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEventInfo, ReceiptInfo, (OrderEventInfo, ReceiptInfo)]#OnTimerContext, out: Collector[(OrderEventInfo, ReceiptInfo)]): Unit = {

      if (payState.value() != null) {
        ctx.output[OrderEventInfo](unMatchReceipt,payState.value())
        payState.clear()
      }

      if (receiptState.value() != null) {

        ctx.output[ReceiptInfo](unMatchPay, receiptState.value())
        receiptState.clear()
      }
    }
  }


}

