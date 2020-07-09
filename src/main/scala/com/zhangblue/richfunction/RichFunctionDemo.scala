package com.zhangblue.richfunction

import java.util.Properties

import com.zhangblue.entity.TemperatureSensor
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.configuration.{ConfigOption, Configuration}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}

/**
 * 自定义富函数demo
 */
object RichFunctionDemo {
  def main(args: Array[String]): Unit = {
    //1. 环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2. 定义kafka作为source，计算，并显示结果
    val topic = "my-flink-topic"
    val valueDeserializationSchema: DeserializationSchema[String] = new SimpleStringSchema()
    val propsConsumer: Properties = new Properties()
    propsConsumer.load(this.getClass.getClassLoader.getResourceAsStream("kafka/home-consumer.properties"))
    val kafkaSource = env.addSource(new FlinkKafkaConsumer011[String](topic, valueDeserializationSchema, propsConsumer))

    val srcDataStream: DataStream[TemperatureSensor] = kafkaSource.filter(_.trim.nonEmpty).map(fun = linedata => {
      val arr = linedata.split(",")
      val id = arr(0).trim
      val timestamp = arr(1).trim.toLong
      val name = arr(2).trim
      val temperature = arr(3).trim.toDouble
      val location = arr(4).trim
      TemperatureSensor(id, timestamp, name, temperature, location)
    })


    val propsProducer: Properties = new Properties()
    propsProducer.load(this.getClass.getClassLoader.getResourceAsStream("kafka/home-producer.properties"))
    srcDataStream.flatMap(new MyRichFlatMapFunction(propsProducer))

    //3. 启动
    env.execute("source from kafka demo")
  }

  /**
   * 自定义flatmap复函数
   *
   * @param properties kafkaProducer配置
   */
  private class MyRichFlatMapFunction(properties: Properties) extends RichFlatMapFunction[TemperatureSensor, TemperatureSensor] {

    private var producer: KafkaProducer[String, String] = _
    private var topic: String = _

    override def open(parameters: Configuration): Unit = {
      //从配置文件中获取topic名称
      topic = parameters.getString(new ConfigOption[String]("topic"), "default-topic")
      producer = new KafkaProducer[String, String](properties)
    }

    override def flatMap(value: TemperatureSensor, out: Collector[TemperatureSensor]): Unit = {
      val isNormal = value.temperature <= 37
      if (isNormal) {
        out.collect(value)
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
