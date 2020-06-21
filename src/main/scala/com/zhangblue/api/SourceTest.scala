package com.zhangblue.api

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    val evn = StreamExecutionEnvironment.getExecutionEnvironment
    evn.setParallelism(1)

    //从内存中读取数据
    val stream1: DataStream[SensorReading] = evn.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_8", 1547718205, 38.1),
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_1", 1547718199, 35.8)
    ))

    //从文件中读取数据
    //val stream2: DataStream[String] = evn.readTextFile("D:\\software\\workspace\\FlinkTutorial\\src\\main\\resources\\sensor")


    //从socket文本流
    // val stream3: DataStream[String] = evn.socketTextStream("localhost", 7777)


    //从kafka读取数据
    //    val properties = new Properties()
    //    properties.setProperty("bootstrap.servers", "localhost:9092")
    //    properties.setProperty("group.id", "consumer-group")
    //    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    //    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    //    properties.setProperty("auto.offset.reset", "latest")
    //
    //    val stream4 = evn.addSource(new FlinkKafkaConsumer011[String]("my-topic", new SimpleStringSchema(), properties))


    //自定义source
    val stream5 = evn.addSource(new MySource())

    stream5.print("stream5")
    evn.execute("source demo")

  }
}

//实现一个自定义的SourceFunction， 自动生成测试数据
class MySource() extends SourceFunction[SensorReading] {
  //定义一个flag，表示数据源是否正常运行
  var running: Boolean = true

  override def cancel(): Unit = running = false

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    //定义一个随机数发生器
    val random = Random
    //随机生成10个传感器的温度值，并且不停更新（随机上线波动）
    // 首先生成10个传感器的初始温度
    var currentTemps = 1.to(10).map(i => ("sensor_" + i, 60 + random.nextGaussian() * 20))

    //无限循环，生成随机数据
    while (running) {
      //在单钱温度基础上，随机生成微小波动
      currentTemps = currentTemps.map(data => (data._1, data._2 + random.nextGaussian()))
      //获取当前系统时间
      val currentTime = System.currentTimeMillis();
      //包装成样例类，适用sourceContext发出数据
      currentTemps.foreach(data => sourceContext.collect(new SensorReading(data._1, currentTime, data._2)))
      //定义间隔时间
      TimeUnit.SECONDS.sleep(1L)
    }
  }
}
