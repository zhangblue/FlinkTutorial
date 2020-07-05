package com.zhangblue.sink

import java.util.Properties

import com.zhangblue.entity.TemperatureSensor
import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
 * Flink流处理写入kafka sink demo
 *
 * @author di.zhang
 * @date 2020/7/5
 * @time 17:42
 **/
object SinkKafkaDemo {
  def main(args: Array[String]): Unit = {
    //需求，求出传感器迄今为止探索到的最高温度的传感器数据
    //1. 环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //获取类加载器
    val classLoader = this.getClass.getClassLoader;

    //2. 读取kafka源，根据传感器的id进行分组，求每组中最大的温度值，并显示结果
    val topic = "my-topic"
    val valueDeserializationSchema: DeserializationSchema[String] = new SimpleStringSchema()
    val props: Properties = new Properties()
    props.load(classLoader.getResourceAsStream("kafka/home-consumer.properties"))
    val kafkaSource = env.addSource(new FlinkKafkaConsumer011[String](topic, valueDeserializationSchema, props))

    val srcDataStream: DataStream[TemperatureSensor] = kafkaSource.filter(_.trim.nonEmpty).map(fun = lineData => {
      val arr = lineData.split(",")
      val id = arr(0).trim
      val timestamp = arr(1).trim.toLong
      val name = arr(2).trim
      val temperature = arr(3).trim.toDouble
      val location = arr(4).trim
      TemperatureSensor(id, timestamp, name, temperature, location)
    }).keyBy("id").maxBy("temperature")


    //将DataStream 输出到Kafka中
    val producerConfig: Properties = new Properties();
    producerConfig.load(classLoader.getResourceAsStream("kafka/home-producer.properties"))
    srcDataStream.map(data => data.toString).addSink {
      new FlinkKafkaProducer011[String]("flink-target-topic", new SimpleStringSchema(), producerConfig)
    }
    //3. 启动
    env.execute(this.getClass.getSimpleName)
  }
}
