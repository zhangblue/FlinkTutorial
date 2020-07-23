package com.zhangblue.test

import java.util.Properties

import com.zhangblue.entity.TemperatureSensor
import com.zhangblue.function.MyMapFunction
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
 * @author di.zhang
 * @date 2020/7/23
 * @time 19:53
 **/
object SourceFromKafkaSinkToKafkaWithWindowDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val props: Properties = new Properties()
    props.setProperty("zookeeper.connect", "vm-118:2181,vm-128:2181,vm-129:2181")
    props.setProperty("bootstrap.servers", "vm-118:9092,vm-128:9092,vm-129:9092")
    props.setProperty("group.id", "test-consumer-group")
    props.setProperty("auto.offset.reset", "latest")
    val valueDeserializationSchema: DeserializationSchema[String] = new SimpleStringSchema()

    val kafkaInputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("zzzzzz-test-100", valueDeserializationSchema, props))
    val dataStream: DataStream[TemperatureSensor] = kafkaInputStream.map(new MyMapFunction)

    val dataOutput: DataStream[String] = dataStream.keyBy(_.id).timeWindow(Time.seconds(30)).reduce(new MyReduce).map(_.toString)


    dataOutput.addSink(new FlinkKafkaProducer011[String]("vm-118:9092,vm-128:9092,vm-129:9092", "flink-output-topic", new SimpleStringSchema()))
    dataOutput.print("data = ")

    env.execute("SourceFromKafkaSinkToKafkaWithWindowDemo")
  }

  private class MyReduce extends ReduceFunction[TemperatureSensor] {
    override def reduce(value1: TemperatureSensor, value2: TemperatureSensor): TemperatureSensor = {
      TemperatureSensor(value1.id, value1.timestamp.min(value2.timestamp), value1.name, value1.temperature.max(value2.temperature), value2.location)
    }
  }

}
