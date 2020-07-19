package com.zhangblue.window

import com.zhangblue.entity.TemperatureSensor
import com.zhangblue.source.MySourceFromFile
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author di.zhang
 * @date 2020/7/16
 * @time 11:43
 **/
object WindowDemo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputDataStream: DataStream[TemperatureSensor] = env.addSource(new MySourceFromFile(1, "src/main/resources/temperature-sensor.txt"))

    inputDataStream
      .keyBy(_.id)
      //.window(EventTimeSessionWindows.withGap(Time.minutes(1)))//会话窗口
      //.timeWindow(Time.hours(1))//滚动窗口
      .timeWindow(Time.hours(1), Time.minutes(1)) //滑动窗口


    env.execute("TumbingWindowDemo")
  }

}
