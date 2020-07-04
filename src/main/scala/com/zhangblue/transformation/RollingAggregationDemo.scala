package com.zhangblue.transformation

import java.util.Properties

import com.zhangblue.entity.TemperatureSensor
import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * 滚动聚合算子演示
 *
 */
object RollingAggregationDemo {
  def main(args: Array[String]): Unit = {
    //需求，求出传感器迄今为止探索到的最高温度的传感器数据
    //1. 环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //2. 读取kafka源，根据传感器的id进行分组，求每组中最大的温度值，并显示结果
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
    }).keyBy("id").maxBy("temperature").print("最高温度的传感器信息为：")
    //3. 启动
    env.execute(this.getClass.getSimpleName)
  }
}
