package com.mzh.flink.hot_items_analysis.app

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

case class ApacheLog(ip: String, userId: String, eventTime: Long, method: String, url: String)

case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object NetworkFlow {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val logDS: DataStream[String] = env.readTextFile("./data/apache.log")

    val apacheLogDS: DataStream[ApacheLog] = logDS.map(
      log => {
        val logArray: Array[String] = log.split(" ")
        //ip:String,userId:String,eventTime:Long,method:String,url:String
        val ip: String = logArray(0).trim
        val userId: String = logArray(1).trim

        val format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val eventTime: Long = format.parse(logArray(3).trim).getTime

        val method: String = logArray(5).trim
        val url: String = logArray(6).trim

        ApacheLog(ip, userId, eventTime, method, url)
      }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLog](Time.seconds(1)) {
      override def extractTimestamp(t: ApacheLog): Long = t.eventTime
    }).filter( data => {
      val pattern = "^((?!\\.(css|js)$).)*$".r
      (pattern findFirstIn data.url).nonEmpty
    })

    val topDS: DataStream[String] = apacheLogDS.keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .aggregate(new AggCount(), new WindowFunctionResult())
      .keyBy(_.windowEnd)
      .process(new TopNPage(5))

    topDS.print()

    env.execute()
  }
}

class AggCount extends AggregateFunction[ApacheLog, Long, Long] {
  override def createAccumulator(): Long = 0

  override def add(in: ApacheLog, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class WindowFunctionResult extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class TopNPage(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
  var urlState: ListState[UrlViewCount] = _


  override def open(parameters: Configuration): Unit = {
    urlState = getRuntimeContext.getListState[UrlViewCount](new ListStateDescriptor[UrlViewCount]("url-state", classOf[UrlViewCount]))
  }

  override def processElement(input: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
    urlState.add(input)

    context.timerService().registerEventTimeTimer(input.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val urlIter: util.Iterator[UrlViewCount] = urlState.get().iterator()

    val urlList: ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]

    while (urlIter.hasNext) {
      urlList += urlIter.next()
    }

    val topUrls: ListBuffer[UrlViewCount] = urlList.sortWith(_.count>_.count).take(topSize)

    val result: StringBuilder = new StringBuilder

    result.append("============================\n")
    result.append("时间：").append(new Timestamp(timestamp-1)).append("\n")

    for(i <- topUrls.indices){
      result.append("NO:").append(i)
        .append("\tURL:").append(topUrls(i).url)
        .append("\t访问量:").append(topUrls(i).count).append("\n")
    }

    Thread.sleep(1000)

    urlState.clear()

    out.collect(result.toString())

  }
}
