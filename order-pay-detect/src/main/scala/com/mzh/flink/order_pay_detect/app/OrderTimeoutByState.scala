package com.mzh.flink.order_pay_detect.app


import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


object OrderTimeoutByState {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sourceDS: DataStream[String] = env.readTextFile("./data/OrderLog.csv")
    //val sourceDS: DataStream[String] =env.socketTextStream("localhost",7777)

    val orderDS: DataStream[OrderEvent] = sourceDS.map(
      line => {
        val lineArray: Array[String] = line.split(",")
        val orderId: Long = lineArray(0).trim.toLong
        val eventType: String = lineArray(1).trim
        val eventTime: Long = lineArray(3).trim.toLong

        OrderEvent(orderId, eventType, eventTime)
      }
    ).assignAscendingTimestamps(_.eventTime * 1000L)


    val keyByDS: KeyedStream[OrderEvent, Long] = orderDS.keyBy(_.orderId)


    keyByDS.process(new OrdeyPayTimeoutWarming()).print()

    env.execute("Order Pay Timeout Job")

  }
}

class OrdeyPayTimeoutWarming extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {

  private lazy val isPayState: ValueState[Boolean] = getRuntimeContext.getState[Boolean](new ValueStateDescriptor[Boolean]("is-Pay", classOf[Boolean]))

  private lazy val createTimeState: ValueState[Long] = getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("createTime",classOf[Long]))
  private lazy val payTimeState: ValueState[Long] = getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("payTimeState",classOf[Long]))

  override def processElement(value: OrderEvent, context: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, collector: Collector[OrderResult]): Unit = {
    val isPay: Boolean = isPayState.value()

    if ("create".equals(value.eventType) && !isPay) {
      context.timerService().registerEventTimeTimer(value.eventTime * 1000L + 15 * 60 * 1000L)
      createTimeState.update(value.eventTime)
    } else if ("pay".equals(value.eventType)) {
      isPayState.update(true)
      payTimeState.update(value.eventTime)
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    val orderId: Long = ctx.getCurrentKey

    val createTime: Long = createTimeState.value()
    val payTime: Long = payTimeState.value()

    println((payTime-createTime)/60)

    println((timestamp / 1000 - createTime) / 60)

    if (isPayState.value()) {
      out.collect(OrderResult(orderId, "Order Pay Successfully"))
    } else {
      out.collect(OrderResult(orderId, "Order Pay Timeout"))
    }

    isPayState.clear()
  }
}


