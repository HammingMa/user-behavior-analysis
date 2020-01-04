package com.mzh.flink.login_fail_delect.app

import java.sql.Timestamp
import java.{lang, util}

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


object LogFailWithCEP {
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


    val keyDS: KeyedStream[LoginLog, Long] = loginDS.keyBy(_.userId)

    val pattern: Pattern[LoginLog, LoginLog] = Pattern.begin[LoginLog]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail")
      .within(Time.seconds(2))

    val ps: PatternStream[LoginLog] = CEP.pattern(keyDS, pattern)

    ps.select(new LoginFailMatch())
      .print()


    env.execute("Login Fail With CEP Job")


  }
}

class LoginFailMatch extends PatternSelectFunction[LoginLog, FailWarming] {
  override def select(map: util.Map[String, util.List[LoginLog]]): FailWarming = {
    val begin: LoginLog = map.get("begin").iterator().next()
    val next: LoginLog = map.get("next").iterator().next()

    val userId: Long = begin.userId
    val firstFailTime: String = new Timestamp(begin.eventTime*1000).toString
    val lastFailTime: String = new Timestamp(next.eventTime*1000).toString

    FailWarming(userId, firstFailTime, lastFailTime, "login fail in 2 seconds " + 2 + " times today")
  }
}
