package com.mzh.flink.market_analysis.app

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

case class MarketingUserBehavior(userid:String,behavior:String,channel:String,timestamp: Long)
case class MarketingCountView(windowStart:String,windowEnd:String,behavior:String,channel:String,count:Long)

object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sourceDS: DataStream[MarketingUserBehavior] = env.addSource(new SimulatedEventSource()).assignAscendingTimestamps(_.timestamp)

    sourceDS.filter(_.behavior!="UNINSTALL")
        .map(userBehavior => ((userBehavior.channel,userBehavior.behavior),1L))
        .keyBy(_._1)
        .timeWindow(Time.hours(1),Time.seconds(10))
        .process(new MarketingCountByChannel())
        .print()


    env.execute("App Market By Channel Job")

  }
}

class SimulatedEventSource extends SourceFunction[MarketingUserBehavior]{

  private var running = true

  private val channelSeq: Seq[String] = Seq("AppStore","AppStore","AppStore","AppStore", "XiaomiStore", "XiaomiStore", "HuaweiStore","HuaweiStore","HuaweiStore", "weibo", "wechat", "tieba")
  private val behaviorSeq: Seq[String] = Seq("BROWSE", "BROWSE", "BROWSE", "BROWSE", "CLICK","CLICK","CLICK", "INSTALL", "UNINSTALL")

  private val random = new Random()

  override def run(sourceContext: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    val maxElements = Long.MaxValue
    val count = 0L

    while (running&& count<maxElements){
      val userId: String = UUID.randomUUID().toString
      val behavior: String = behaviorSeq(random.nextInt(behaviorSeq.size))
      val channel: String = channelSeq(random.nextInt(channelSeq.size))
      val timestamp: Long = System.currentTimeMillis()

      sourceContext.collect(MarketingUserBehavior(userId,behavior,channel,timestamp))

      TimeUnit.MILLISECONDS.sleep(10)
    }

  }

  override def cancel(): Unit = {
    running=false
  }
}

class MarketingCountByChannel extends ProcessWindowFunction[((String,String),Long),MarketingCountView,(String,String),TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingCountView]): Unit = {
    val windowStart: String = new Timestamp(context.window.getStart).toString
    val windowEnd: String = new Timestamp(context.window.getEnd).toString
    val channel: String = key._1
    val behavior: String = key._2
    val count: Int = elements.size

    out.collect(MarketingCountView(windowStart,windowEnd,channel,behavior,count))
  }
}


