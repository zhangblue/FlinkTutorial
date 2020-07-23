package com.zhangblue.test

import java.util.Properties

import com.zhangblue.entity.TemperatureSensor
import com.zhangblue.function.MyMapFunction
import com.zhangblue.richfunction.MyJdbcRichSink
import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * 从kafka读取数据写入db
 *
 * @author di.zhang
 * @date 2020/7/23
 * @time 11:44
 **/
object SourceFromKafkaSinkToDb {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val valueDeserializationSchema: DeserializationSchema[String] = new SimpleStringSchema()
    val props: Properties = new Properties()
    props.setProperty("zookeeper.connect", "localhost:2181")
    props.setProperty("bootstrap.servers", "localhost:9092")
    props.setProperty("group.id", "test-consumer-group")
    props.setProperty("auto.offset.reset", "latest")

    val kafkaInputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("my-topic", valueDeserializationSchema, props))
    val dataStream: DataStream[TemperatureSensor] = kafkaInputStream.map(new MyMapFunction)

    dataStream.addSink(new MyJdbcRichSink)
    dataStream.print("data = ")
    env.execute("SourceFromKafkaSinkToDb")
  }
}


