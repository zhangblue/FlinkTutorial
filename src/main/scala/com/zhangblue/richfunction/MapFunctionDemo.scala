package com.zhangblue.richfunction

import java.util.Properties

import com.zhangblue.entity.TemperatureSensor
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * @author di.zhang
 * @date 2020/7/13
 * @time 23:37
 **/
object MapFunctionDemo {

  def main(args: Array[String]): Unit = {

    //1. 环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2. 定义kafka作为source，计算，并显示结果
    val valueDeserializationSchema: DeserializationSchema[String] = new SimpleStringSchema()
    val propsConsumer: Properties = new Properties()
    propsConsumer.load(this.getClass.getClassLoader.getResourceAsStream("kafka/local-consumer.properties"))
    val kafkaSource = env.addSource(new FlinkKafkaConsumer011[String]("my-topic", valueDeserializationSchema, propsConsumer))

    val srcDataStream: DataStream[TemperatureSensor] = kafkaSource.filter(_.trim.nonEmpty).map(fun = linedata => {
      val arr = linedata.split(",")
      val id = arr(0).trim
      val timestamp = arr(1).trim.toLong
      val name = arr(2).trim
      val temperature = arr(3).trim.toDouble
      val location = arr(4).trim
      TemperatureSensor(id, timestamp, name, temperature, location)
    })

    srcDataStream.map(new MyMapper).map(data => data._2 > 37).print("value = ")


  }

  private class MyMapper extends MapFunction[TemperatureSensor, (String, Double)] {
    override def map(value: TemperatureSensor): (String, Double) = (value.id, value.temperature)
  }


}
