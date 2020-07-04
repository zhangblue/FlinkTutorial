package com.zhangblue.transformation

import java.util.Properties

import com.zhangblue.entity.TemperatureSensor
import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector

/**
 * connect demo
 */
object ConnectedStreamDemo {
  def main(args: Array[String]): Unit = {
    //需求：基于之前的侧输出流的案例，将主输出流与测输出流集中寄来，进行统一处理
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

    //针对DataStream调用侧输出流进行处理
    val outputTag: OutputTag[TemperatureSensor] = OutputTag("temperature_exception")
    val resultDataStream: DataStream[TemperatureSensor] = srcDataStream.process[TemperatureSensor](new MyProcessFunction(outputTag))

    //主输出流
    val mainDataStream: DataStream[(String, String)] = resultDataStream.map(data => (data.id, data.name))
    //测输出流
    val sideOutputStream: DataStream[(String, String, Double, Long)] = resultDataStream.getSideOutput(outputTag).map(data => (data.id, data.name, data.temperature, data.timestamp))

    //合并两个流中的数据,获得ConnectedStreams
    val connDataStream: ConnectedStreams[(String, String), (String, String, Double, Long)] = mainDataStream.connect(sideOutputStream)

    //ConnectedStreams进行集中式处理 
    val finalDataStream: DataStream[String] = connDataStream.map(
      mStreamData => s"传感器id = ${mStreamData._1} , 旅客名 = ${mStreamData._2} , 您的体温正常",
      oStreamData => s"传感器id = ${oStreamData._1} , 旅客名 = ${oStreamData._2} , 您的体温异常 = ${oStreamData._3}, 时间 = ${oStreamData._4}"
    )

    finalDataStream.print()

    env.execute(this.getClass.getSimpleName)
  }

  /**
   * 自定义一个ProcessFunction子类
   *
   * @param outputTag 用来给侧输出流中的数据添加标签
   *
   */
  private class MyProcessFunction(outputTag: OutputTag[TemperatureSensor]) extends ProcessFunction[TemperatureSensor, TemperatureSensor] {
    /**
     * 每分析DataStream中的一个元素，下述方法就执行一次
     *
     * @param value 当前的元素
     * @param ctx   上下文信息，用于向侧输出流中写入数据
     * @param out   用于向主输出流中写入数据
     */
    override def processElement(value: TemperatureSensor, ctx: ProcessFunction[TemperatureSensor, TemperatureSensor]#Context, out: Collector[TemperatureSensor]): Unit = {
      if (value.temperature < 37) {
        //取出体温正常的信息
        out.collect(value)
      } else {
        //取出体温异常的信息
        ctx.output[TemperatureSensor](outputTag, value)
      }
    }
  }

}
