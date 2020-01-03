package com.mzh.flink.hot_items_analysis.app

import java.sql.Timestamp
import java.util.Properties

import com.mzh.flink.hot_items_analysis.app.model.{ItemViewCount, UserBehavior}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object HotItems {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    //设置事件时间为watermark的标准
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    //val fileDS: DataStream[String] = env.readTextFile("./data/UserBehavior.csv")


    val properties = new Properties()
    properties.setProperty("bootstrap.servers","hdp1:9092,hdp2:9092,hdp3:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val kafkaSource: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("UserBehavior",new SimpleStringSchema(),properties)

    val kafkaDS: DataStream[String] = env.addSource(kafkaSource)


    val userBehaviorDS: DataStream[UserBehavior] = kafkaDS.map(
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
    ).assignAscendingTimestamps(_.timestamp * 1000) //设置属性timestamp为事件时间

    val topNHotItemsDS: DataStream[String] = userBehaviorDS.filter(userBehavior => "pv".equals(userBehavior.behavior))
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(10))
      .aggregate(new AggCount, new WindowFunctionResult())
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(3))


    topNHotItemsDS.print()

    env.execute("hot items job")

  }
}

class AggCount extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1L

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class WindowFunctionResult extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long,
                     window: TimeWindow,
                     input: Iterable[Long],
                     out: Collector[ItemViewCount]): Unit = {

    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}


class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
  var itemListState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemListState = getRuntimeContext.getListState[ItemViewCount](new ListStateDescriptor[ItemViewCount]("item-list", classOf[ItemViewCount]))

  }

  override def processElement(itemViewCount: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    itemListState.add(itemViewCount)

    context.timerService().registerEventTimeTimer(itemViewCount.windowEnd + 1)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {

    val itembuffer: ListBuffer[ItemViewCount] = ListBuffer()

    import scala.collection.JavaConversions._

    for (item <- itemListState.get()) {
      itembuffer.append(item)
    }

    val topNItem: ListBuffer[ItemViewCount] = itembuffer.sortWith((x, y) => x.count > y.count).take(topSize)

    val result: StringBuilder = new StringBuilder

    result.append("=======================\n")

    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

    var i = 1

    for (elem <- topNItem) {

      result.append("No:").append(i)
        .append("\t商品编号：").append(elem.itemId)
        .append("\t浏览量：").append(elem.count)
        .append("\n")

      i+=1

    }

    Thread.sleep(1000)

    out.collect(result.toString())

  }
}


