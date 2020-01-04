package com.mzh.flink.order_pay_detect.app

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)

case class OrderResult(orderId: Long, result: String)

object OrderTimeOut {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sourceDS: DataStream[String] = env.readTextFile("./data/OrderLog.csv")

    val orderDS: DataStream[OrderEvent] = sourceDS.map(
      line => {
        val lineArray: Array[String] = line.split(",")
        val orderId: Long = lineArray(0).trim.toLong
        val eventType: String = lineArray(1).trim
        val eventTime: Long = lineArray(3).trim.toLong

        OrderEvent(orderId, eventType, eventTime)
      }
    ).assignAscendingTimestamps(_.eventTime * 1000)


    val keyByDS: KeyedStream[OrderEvent, Long] = orderDS.keyBy(_.orderId)

    val orderPayPattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(15))

    val orderTimeoutOutput: OutputTag[OrderResult] = new OutputTag[OrderResult]("orderTimeout")

    val orderPS: PatternStream[OrderEvent] = CEP.pattern(keyByDS,orderPayPattern)

    val orderPayDS: DataStream[OrderResult] = orderPS.select(orderTimeoutOutput,new OrderTimeoutSelect(),new OrderPaySelect())

    orderPayDS.print("pay")

    orderPayDS.getSideOutput(orderTimeoutOutput).print("timeout")
    
    
    env.execute("Order Pay Timeout")
    
  }
}

class OrderTimeoutSelect extends PatternTimeoutFunction[OrderEvent,OrderResult]{
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val orderId: Long = map.get("begin").iterator().next().orderId
    OrderResult(orderId,"timeout")
  }

}

class OrderPaySelect extends PatternSelectFunction [OrderEvent,OrderResult]{
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val orderId: Long = map.get("follow").iterator().next().orderId

    OrderResult(orderId,"successfully")
  }

}
