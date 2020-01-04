package com.mzh.flink.login_fail_delect.app

import java.sql.Timestamp
import java.{lang, util}

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

case class LoginLog(userId: Long, ip: String, eventType: String, eventTime: Long)

case class FailWarming(userId: Long, firstFailTime: String, lastFailTime: String, msg: String)

object LogFail {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val sourceDS: DataStream[String] = env.readTextFile("./data/LoginLog.csv")

    val loginDS: DataStream[LoginLog] = sourceDS.map(
      line => {
        val lineArray: Array[String] = line.split(",")
        val userId: Long = lineArray(0).trim.toLong
        val ip: String = lineArray(1).trim
        val eventType: String = lineArray(2).trim
        val eventTime: Long = lineArray(3).trim.toLong
        LoginLog(userId, ip, eventType, eventTime)
      }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginLog](Time.seconds(3)) {
      override def extractTimestamp(t: LoginLog): Long = {
        t.eventTime * 1000
      }
    })


    loginDS.keyBy(_.userId)
      .process(new LoginFailWarming(2))
        .print("logFail")


    env.execute("Log Fail Job")


  }
}

class LoginFailWarming(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginLog, FailWarming] {

  private lazy val loginListState: ListState[LoginLog] = getRuntimeContext.getListState[LoginLog](new ListStateDescriptor[LoginLog]("login-state", classOf[LoginLog]))

  override def processElement(value: LoginLog, context: KeyedProcessFunction[Long, LoginLog, FailWarming]#Context, collector: Collector[FailWarming]): Unit = {
    val loginList: lang.Iterable[LoginLog] = loginListState.get()
    if ("fail".equals(value.eventType)) {
      if (!loginList.iterator().hasNext) {
        context.timerService().registerEventTimeTimer(value.eventTime * 1000 + 2000L)
      }
      loginListState.add(value)
    } else {
      loginListState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginLog, FailWarming]#OnTimerContext, out: Collector[FailWarming]): Unit = {
    var loginBuff: ListBuffer[LoginLog] = new ListBuffer[LoginLog]()

    val iter: util.Iterator[LoginLog] = loginListState.get.iterator()

    while (iter.hasNext) {
      loginBuff += iter.next()
    }

    loginListState.clear()

    if (loginBuff.size > maxFailTimes) {
      val userId: Long = loginBuff.head.userId
      val firstFailTime: String = new Timestamp(loginBuff.head.eventTime*1000).toString
      val lastFailTime: String = new Timestamp(loginBuff.last.eventTime*1000).toString

      out.collect(FailWarming(userId, firstFailTime, lastFailTime, "login fail in 2 seconds " + loginBuff.size + " times today"))
    }

  }
}
