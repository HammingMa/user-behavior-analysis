package com.mzh.flink.hot_items_analysis.app

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

object PageView {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    //设置事件时间为watermark的标准
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val fileDS: DataStream[String] = env.readTextFile("./data/UserBehavior.csv")

    val userBehaviorDS: DataStream[UserBehavior] = fileDS.map(
      data => {
        //userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long
        val dataArray: Array[String] = data.split(",")
        val userId: Long = dataArray(0).trim.toLong
        val itemId: Long = dataArray(1).trim.toLong
        val categoryId: Int = dataArray(2).trim.toInt
        val behavior: String = dataArray(3).trim
        val timestamp: Long = dataArray(4).trim.toLong

        UserBehavior(userId, itemId, categoryId, behavior, timestamp)
      }
    ).assignAscendingTimestamps(_.timestamp * 1000)

    val pvDS: DataStream[(String, Int)] = userBehaviorDS.filter(userBehavior => "pv".equals(userBehavior.behavior))
      .map(x => ("pv", 1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)

    pvDS.print()

    env.execute(" pv job")


  }
}
