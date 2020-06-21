package com.zhangblue.api

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

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
    val stream2: DataStream[String] = evn.readTextFile("D:\\software\\workspace\\FlinkTutorial\\src\\main\\resources\\sensor")


    //从socket文本流
    val stream3: DataStream[String] = evn.socketTextStream("localhost", 7777)


    //从kafka读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val stream4 = evn.addSource(new FlinkKafkaConsumer011[String]("topic1", new SimpleStringSchema(), properties))
    stream2.print("stream2")
    evn.execute("source demo")

  }
}
