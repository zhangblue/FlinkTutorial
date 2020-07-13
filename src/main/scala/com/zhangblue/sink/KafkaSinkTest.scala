package com.zhangblue.sink

import com.zhangblue.entity.TemperatureSensor
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

/**
 * @author di.zhang
 * @date 2020/7/13
 * @time 23:57
 **/
object KafkaSinkTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.readTextFile("src/main/resources/temperature-sensor.txt")


    val dataStream: DataStream[TemperatureSensor] = inputStream.map(new MyMapFunction())


    dataStream.map(data => data.toString).addSink(new FlinkKafkaProducer011[String]("localhost:9092", "output-topic", new SimpleStringSchema()))

    env.execute("KafkaSinkTestDemo")


  }

  private class MyMapFunction extends MapFunction[String, TemperatureSensor] {
    override def map(value: String): TemperatureSensor = {
      val splitData = value.split(",")
      TemperatureSensor(splitData(0), splitData(1).toLong, splitData(2), splitData(3).toDouble, splitData(4))
    }
  }

}
