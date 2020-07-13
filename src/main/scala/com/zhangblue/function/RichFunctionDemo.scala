package com.zhangblue.function

import java.util.Properties

import com.zhangblue.entity.TemperatureSensor
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * 自定义富函数demo
 */
object RichFunctionDemo {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println("exist!!!")
      System.exit(1)
    }

    val parameter: ParameterTool = ParameterTool.fromArgs(args)

    //1. 环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(parameter)

    //2. 定义kafka作为source，计算，并显示结果
    val consumerTopic = parameter.get("consumer_topic_name")
    val valueDeserializationSchema: DeserializationSchema[String] = new SimpleStringSchema()
    val propsConsumer: Properties = new Properties()
    propsConsumer.load(this.getClass.getClassLoader.getResourceAsStream("kafka/local-consumer.properties"))
    val kafkaSource = env.addSource(new FlinkKafkaConsumer011[String](consumerTopic, valueDeserializationSchema, propsConsumer))

    val srcDataStream: DataStream[TemperatureSensor] = kafkaSource.filter(_.trim.nonEmpty).map(fun = linedata => {
      val arr = linedata.split(",")
      val id = arr(0).trim
      val timestamp = arr(1).trim.toLong
      val name = arr(2).trim
      val temperature = arr(3).trim.toDouble
      val location = arr(4).trim
      TemperatureSensor(id, timestamp, name, temperature, location)
    })


    srcDataStream.print("vvvvvvv = ")
    val propsProducer: Properties = new Properties()
    propsProducer.load(this.getClass.getClassLoader.getResourceAsStream("kafka/local-producer.properties"))
    val flatDataStream: DataStream[String] = srcDataStream.flatMap(new MyRichFlatMapFunction(propsProducer))

    flatDataStream.print("value= ")
    //3. 启动
    env.execute("source from kafka demo")
  }

  /**
   * 自定义flatmap复函数
   *
   * @param properties kafkaProducer配置
   */
  private class MyRichFlatMapFunction(properties: Properties) extends RichFlatMapFunction[TemperatureSensor, String] {

    private var producer: KafkaProducer[String, String] = _
    private val topic: String = "output-topic"

    override def open(parameters: Configuration): Unit = {
      println("=======" + properties)

      //从配置文件中获取topic名称
      producer = new KafkaProducer[String, String](properties)


    }

    override def flatMap(value: TemperatureSensor, out: Collector[String]): Unit = {
      val isNormal = value.temperature <= 37
      if (isNormal) {
        out.collect(value.toString)
      } else {
        val producerRecord: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, value.name, value.temperature.toString)
        producer.send(producerRecord)
      }
    }

    override def close(): Unit = {
      if (producer != null) {
        producer.close()
      }
    }
  }

}
