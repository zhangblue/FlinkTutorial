package com.zhangblue.window

import java.util.Properties

import com.zhangblue.entity.TemperatureSensor
import org.apache.flink.api.common.functions.{MapFunction, ReduceFunction}
import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * 滑动窗口demo
 * @author di.zhang
 * @date 2020/7/20
 * @time 23:58
 **/
object SlidingWindowsDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val topic = "my-topic"
    val valueDeserializationSchema: DeserializationSchema[String] = new SimpleStringSchema()
    val props: Properties = new Properties()
    props.load(this.getClass.getClassLoader.getResourceAsStream("kafka/local-consumer.properties"))

    val inputDataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String](topic, valueDeserializationSchema, props)).filter(_.nonEmpty)

    val resultStream: DataStream[TemperatureSensor] = inputDataStream.map(new MyMapFunction)
      .keyBy(_.id)
      .timeWindow(Time.seconds(15), Time.seconds(5))
      .reduce(new MyReduceFunction)


    resultStream.print("value = ")

    env.execute("TumbingWindowDemo")
  }

  /**
   * 根据id， 获取 最大的时间戳和最高的温度，
   */
  private class MyReduceFunction extends ReduceFunction[TemperatureSensor] {
    override def reduce(value1: TemperatureSensor, value2: TemperatureSensor): TemperatureSensor = {
      TemperatureSensor(value1.id, value1.timestamp.max(value2.timestamp), value1.name, value1.temperature.max(value2.temperature), value1.location)
    }
  }

  /**
   * 将kakfa中读出的字符串转换成TemperatureSensor对象
   */
  private class MyMapFunction extends MapFunction[String, TemperatureSensor] {
    override def map(value: String): TemperatureSensor = {
      val arr = value.split(",")
      val id = arr(0).trim
      val timestamp = arr(1).trim.toLong
      val name = arr(2).trim
      val temperature = arr(3).trim.toDouble
      val location = arr(4).trim
      TemperatureSensor(id, timestamp, name, temperature, location)
    }
  }

}
