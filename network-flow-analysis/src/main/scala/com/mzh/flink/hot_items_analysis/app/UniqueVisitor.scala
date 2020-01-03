package com.mzh.flink.hot_items_analysis.app

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class UvCount(time: String, count: Long)

object UniqueVisitor {

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

    val uvDS: DataStream[UvCount] = userBehaviorDS.filter(userBehavior => "pv".equals(userBehavior.behavior))
      .timeWindowAll(Time.hours(1))
      .apply(new UvCountByWindow())

    uvDS.print()

    env.execute("UV Job")

  }
}

class UvCountByWindow() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    var userIds: Set[Long] with Object = Set[Long]()

    for (userBehavior <- input) {
      userIds += userBehavior.userId
    }

    val uvCount = UvCount(new Timestamp(window.getEnd).toString,userIds.size)

    out.collect(uvCount)

  }
}
