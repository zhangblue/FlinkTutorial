package com.zhangblue.transformation

import java.util.Properties

import com.zhangblue.entity.TemperatureSensor
import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector

/**
 * 侧输出流演示
 *
 * 因split函数过时，使用SideOutPut代替
 */
object SideOutputDemo {
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

    //针对DataStream调用侧输出流进行处理
    val outputTag: OutputTag[TemperatureSensor] = OutputTag("temperature_exception")
    val resultDataStream: DataStream[TemperatureSensor] = srcDataStream.process[TemperatureSensor](new MyProcessFunction(outputTag))

    resultDataStream.print("体温正常的旅客 ： ")
    resultDataStream.getSideOutput(outputTag).print("体温异常的旅客 ： ")

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
