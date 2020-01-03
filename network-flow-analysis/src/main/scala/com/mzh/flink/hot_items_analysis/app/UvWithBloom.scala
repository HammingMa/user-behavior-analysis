package com.mzh.flink.hot_items_analysis.app

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object UvWithBloom {

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

    userBehaviorDS.filter(userBehavior => "pv".equals(userBehavior.behavior))
      .map(data => ("dummyKey", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger())
      .process(new UvCountWithBloom())
      .print()


    env.execute("UV With Bloom Job")
  }

}

class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
  override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.FIRE_AND_PURGE
  }

  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {

  }

}

class Bloom(size: Long) extends Serializable {

  private val bloomSize: Long = if (size > 0) size else 1 << 27

  def hash(value: String, speed: Long): Long = {
    var result: Long = 0L

    for (i <- 0 until value.length) {
      result = result * speed + value(i)
    }

    result & (bloomSize - 1)
  }
}

class UvCountWithBloom() extends ProcessWindowFunction[(String,Long),UvCount,String,TimeWindow]{

  private lazy val jedis = new Jedis("hdp2",6379)

  private lazy val bloom = new Bloom(1<<29)


  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    val storeKey: String = new Timestamp(context.window.getEnd).toString

    var count: Long = 0L

    val preCount =jedis.hget("count",storeKey)
    if(preCount !=null){
      count = preCount.toLong
    }

    val userId: String = elements.last._2.toString

    val offset: Long = bloom.hash(userId,73)

    if(!jedis.getbit(storeKey,offset)){
      jedis.hset("count",storeKey,(count+1).toString)
      jedis.setbit(storeKey,offset,true)

      out.collect(UvCount(storeKey,count+1))
    }else{
      out.collect(UvCount(storeKey,count))
    }

  }


}


