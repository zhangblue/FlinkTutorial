package com.zhangblue.transformation

import java.util.Properties

import com.zhangblue.entity.TemperatureSensor
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * reduce算子
 */
object ReduceDemo {

  def main(args: Array[String]): Unit = {
    //需求：获取每个传感器获取的最新戳与最高的温度
    //1. 环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //2. 读取kafka源，根据传感器的id进行分组，计算每个传感器的最新时间戳与最高温度
    val topic = "my-flink-topic"
    val valueDeserializationSchema: DeserializationSchema[String] = new SimpleStringSchema()
    val props: Properties = new Properties()
    props.load(this.getClass.getClassLoader.getResourceAsStream("kafka/home-consumer.properties"))
    val kafkaSource = env.addSource(new FlinkKafkaConsumer011[String](topic, valueDeserializationSchema, props))

    kafkaSource.filter(_.trim.nonEmpty).map(fun = lineData => {
      val arr = lineData.split(",")
      val id = arr(0).trim
      val timestamp = arr(1).trim.toLong
      val name = arr(2).trim
      val temperature = arr(3).trim.toDouble
      val location = arr(4).trim
      TemperatureSensor(id, timestamp, name, temperature, location)
    }).keyBy(data => data.id).reduce(new MyReduce).print("最终结果 ：")
    //3. 启动
    env.execute(this.getClass.getSimpleName)
  }

  //自定义reduce function
  class MyReduce extends ReduceFunction[TemperatureSensor] {
    override def reduce(value1: TemperatureSensor, value2: TemperatureSensor): TemperatureSensor = {
      TemperatureSensor(value1.id, value1.timestamp.max(value2.timestamp), value1.name, value1.temperature.max(value2.temperature), value1.location)
    }
  }

}

