package com.zhangblue.test

import java.util.Properties

import com.zhangblue.entity.TemperatureSensor
import com.zhangblue.function.MyMapFunction
import com.zhangblue.richfunction.MyJdbcRichSink
import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * 从kafka读取数据， 使用window计算每个key产生的个数， 写入db
 *
 * @author di.zhang
 * @date 2020/7/23
 * @time 14:33
 **/
object SourceFromKafkaSinkToDbWithWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val valueDeserializationSchema: DeserializationSchema[String] = new SimpleStringSchema()
    val props: Properties = new Properties()
    props.load(this.getClass.getClassLoader.getResourceAsStream("kafka/local-consumer.properties"))

    val kafkaInputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("my-topic", valueDeserializationSchema, props))
    val dataStream: DataStream[TemperatureSensor] = kafkaInputStream.map(new MyMapFunction)

    dataStream.keyBy(_.id).timeWindow(Time.seconds(10))

    dataStream.addSink(new MyJdbcRichSink)
    dataStream.print("data = ")
    env.execute("SourceFromKafkaSinkToDb")
  }
}
