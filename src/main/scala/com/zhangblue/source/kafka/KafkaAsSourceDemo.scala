package com.zhangblue.source.kafka

import java.util.Properties

import com.zhangblue.entity.TemperatureSensor
import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * 以kafka中的数据作为dataStream的Source的来源
 */
object KafkaAsSourceDemo {
  def main(args: Array[String]): Unit = {
    //1. 环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2. 定义kafka作为source，计算，并显示结果
    val topic = "my-topic"
    val valueDeserializationSchema: DeserializationSchema[String] = new SimpleStringSchema()
    val props: Properties = new Properties()
    props.load(this.getClass.getClassLoader.getResourceAsStream("kafka/local-consumer.properties"))
    val kafkaSource = env.addSource(new FlinkKafkaConsumer011[String](topic, valueDeserializationSchema, props))

    kafkaSource.filter(_.trim.nonEmpty).map(fun = linedata => {
      val arr = linedata.split(",")
      val id = arr(0).trim
      val timestamp = arr(1).trim.toLong
      val name = arr(2).trim
      val temperature = arr(3).trim.toDouble
      val location = arr(4).trim
      TemperatureSensor(id, timestamp, name, temperature, location)
    }).print("source from kafka : ")

    //3. 启动
    env.execute("source from kafka demo")


  }
}
