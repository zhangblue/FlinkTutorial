package com.zhangblue.source

import java.util.concurrent.TimeUnit

import com.zhangblue.entity.TemperatureSensor
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.io.Source
import scala.util.Random

/**
 * 从文件中读取数据，返回@TemperatureSensor类型的数据
 *
 * @param sleepTimeSec sleep时间， 表示每隔多少秒发送一次数据
 * @param logFilePath  读取文件的位置
 * @author di.zhang
 * @date 2020/7/15
 * @time 21:46
 **/
class MySourceFromFile(sleepTimeSec: Int, logFilePath: String) extends SourceFunction[TemperatureSensor] {

  private var flg = true

  override def run(sourceContext: SourceFunction.SourceContext[TemperatureSensor]): Unit = {
    //1. 读取文件, 封装成TemperatureSensor的实例
    val lst: List[TemperatureSensor] = Source
      .fromFile(logFilePath).getLines().toList.map(lineData => {
      val arr = lineData.split(",")
      val id = arr(0).trim
      val timestamp = arr(1).trim.toLong
      val name = arr(2).trim
      val temperature = arr(3).trim.toDouble
      val location = arr(4).trim
      TemperatureSensor(id, timestamp, name, temperature, location)
    })
    //2. 如果没有cancel，通过循环来模拟每间隔1秒钟向Source发送一条数据。
    while (flg) {
      val randomIndex = Random.nextInt(lst.size)
      val randomInfo = lst(randomIndex)
      sourceContext.collect(randomInfo)
      TimeUnit.SECONDS.sleep(sleepTimeSec)
    }
  }

  override def cancel(): Unit = flg = false
}
