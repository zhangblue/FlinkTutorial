package com.zhangblue.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.zhangblue.entity.TemperatureSensor
import com.zhangblue.richfunction.MyJdbcRichSink
import com.zhangblue.source.MySourceFromFile
import org.apache.flink.api.common.functions.{IterationRuntimeContext, MapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
 * 使用自定义sink链接数据库
 *
 * @author di.zhang
 * @date 2020/7/15
 * @time 21:22
 **/
object DbSinkDemo2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputData: DataStream[TemperatureSensor] = env.addSource(new MySourceFromFile(1, "src/main/resources/temperature-sensor.txt"))

    inputData.addSink(new MyJdbcRichSink)

    env.execute("sink to db demo")
  }


}
