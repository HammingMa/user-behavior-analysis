package com.mzh.flink.market_analysis.app

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

case class CountByProvince(windStart: String, windEnd: String, Province: String, count: Long)

case class BlackListWarming(userId: Long, adId: Long, msg: String)

object AdStatisticsByGeo {

  private val blackListWarmingTag = new OutputTag[BlackListWarming]("BlackListWarming")

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val sourceDS: DataStream[String] = env.readTextFile("./data/AdClickLog.csv")

    val adClickDS: DataStream[AdClickLog] = sourceDS.map(
      line => {
        val lineArray: Array[String] = line.split(",")
        val userId: Long = lineArray(0).trim.toLong
        val adId: Long = lineArray(1).trim.toLong
        val province: String = lineArray(2).trim
        val city: String = lineArray(3).trim
        val timestamp: Long = lineArray(4).trim.toLong
        AdClickLog(userId, adId, province, city, timestamp)
      }
    ).assignAscendingTimestamps(_.timestamp*1000)

    val filterDS: DataStream[AdClickLog] = adClickDS.keyBy(data => (data.userId, data.adId))
      .process(new FilterBlackListUser(100))

    filterDS.getSideOutput(blackListWarmingTag).print("blackListWarming")


    filterDS.keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountWindowResult())
      .print("count")

    env.execute(" Ad Click Job ")

  }

  class FilterBlackListUser(maxCount: Long) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {

    //保存点击次数的状态
    private lazy val countState: ValueState[Long] = getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("count-state", classOf[Long]))
    //保存定时器时间的状态
    private lazy val timerState: ValueState[Long] = getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("timer-state", classOf[Long]))
    //保存是否发送过警告的状态
    private lazy val isSentState: ValueState[Boolean] = getRuntimeContext.getState[Boolean](new ValueStateDescriptor[Boolean]("isSent-state", classOf[Boolean]))

    override def processElement(input: AdClickLog, context: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, collector: Collector[AdClickLog]): Unit = {
      val count: Long = countState.value()

      if (count == 0) {
        val timer: Long = (context.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
        timerState.update(timer)
        context.timerService().registerProcessingTimeTimer(timer)
      }

      if (count > maxCount) {
        if(!isSentState.value()){
          isSentState.update(true)
          context.output(blackListWarmingTag,BlackListWarming(input.userId,input.adId,"click over "+count +" times today"))
        }
        return
      }else{
        countState.update(count+1)
        collector.collect(input)
      }

    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
      if(timestamp == timerState.value()){
        countState.clear()
        timerState.clear()
        isSentState.clear()
      }
    }

  }

}

class AdCountAgg extends AggregateFunction[AdClickLog, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: AdClickLog, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class AdCountWindowResult extends WindowFunction[Long, CountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    val windowStart: String = new Timestamp(window.getStart).toString
    val windowEnd: String = new Timestamp(window.getEnd).toString
    val province: String = key
    val count: Long = input.iterator.next()

    out.collect(new CountByProvince(windowStart, windowEnd, province, count))
  }
}
