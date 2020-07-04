package com.zhangblue.transformation

import java.util.Properties

import com.zhangblue.entity.TemperatureSensor
import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * split select demo
 */
object SplitAndSelectDemo {

  def main(args: Array[String]): Unit = {
    //需求： 按照温度将旅客信息划分为体温正常与异常的两个组
    //环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val topic = "my-flink-topic"
    val valueDeserializationSchema: DeserializationSchema[String] = new SimpleStringSchema()
    val props: Properties = new Properties()
    props.load(this.getClass.getClassLoader.getResourceAsStream("kafka/home-consumer.properties"))
    val kafkaSource = env.addSource(new FlinkKafkaConsumer011[String](topic, valueDeserializationSchema, props))

    //获得source
    val srcDataStream: DataStream[TemperatureSensor] = kafkaSource.filter(_.trim.nonEmpty).map(fun = lineData => {
      val arr = lineData.split(",")
      val id = arr(0).trim
      val timestamp = arr(1).trim.toLong
      val name = arr(2).trim
      val temperature = arr(3).trim.toDouble
      val location = arr(4).trim
      TemperatureSensor(id, timestamp, name, temperature, location)
    })

    //针对体温进行切分
    val splitStream: SplitStream[TemperatureSensor] = srcDataStream.split(data => {
      if (data.temperature > 37) {
        //体温异常
        Seq("exception")
      } else {
        //体温正常
        Seq("normal")
      }
    })

    val normalDataStream: DataStream[TemperatureSensor] = splitStream.select("normal")
    normalDataStream.print("体温正常的旅客 ： ")
    val exceptionDataStream: DataStream[TemperatureSensor] = splitStream.select("exception")
    exceptionDataStream.print("体温异常的旅客 ： ")

    env.execute(this.getClass.getSimpleName)
  }

}
