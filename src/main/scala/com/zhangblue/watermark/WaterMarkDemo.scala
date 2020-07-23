package com.zhangblue.watermark

import java.util.Properties

import com.zhangblue.entity.TemperatureSensor
import com.zhangblue.function.MyMapFunction
import com.zhangblue.window.SlidingWindowsDemo.MyReduceFunction
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * 设置watermark
 *
 * @author di.zhang
 * @date 2020/7/23
 * @time 19:28
 **/
object WaterMarkDemo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //设置使用事件时间作为时间衡量标准

    val topic = "my-topic"
    val valueDeserializationSchema: DeserializationSchema[String] = new SimpleStringSchema()
    val props: Properties = new Properties()
    props.load(this.getClass.getClassLoader.getResourceAsStream("kafka/local-consumer.properties"))

    val inputDataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String](topic, valueDeserializationSchema, props)).filter(_.nonEmpty)

    val resultStream: DataStream[TemperatureSensor] = inputDataStream.map(new MyMapFunction)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[TemperatureSensor](Time.seconds(5)) {
        override def extractTimestamp(element: TemperatureSensor): Long = element.timestamp
      })
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

}
