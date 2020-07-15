package com.zhangblue.sink

import java.sql.PreparedStatement

import com.zhangblue.entity.TemperatureSensor
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.scala._

/**
 * @author di.zhang
 * @date 2020/7/15
 * @time 15:56
 **/
object DbSinkDemo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputData: DataStream[String] = env.readTextFile("src/main/resources/temperature-sensor.txt")

    val dataStream: DataStream[TemperatureSensor] = inputData.map(new MyMapFunction)

    val jdbcConnectionOptions: JdbcConnectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withDriverName("com.mysql.jdbc.Driver").withUrl("jdbc:mysql://localhost:3306/zhangblue_test?characterEncoding=utf8&useSSL=true").withUsername("root").withPassword("12345678").build()

    dataStream.addSink(JdbcSink.sink("insert into temperature_sensor (id,timestamp,name,temperature,location) values (?,?,?,?,?)", new MyJdbcStatementBuilder, jdbcConnectionOptions))

    env.execute("sink to db demo")
  }

  private class MyMapFunction extends MapFunction[String, TemperatureSensor] {
    override def map(value: String): TemperatureSensor = {
      val splitData = value.split(",")
      TemperatureSensor(splitData(0), splitData(1).toLong, splitData(2), splitData(3).toDouble, splitData(4))
    }
  }

  private class MyJdbcStatementBuilder extends JdbcStatementBuilder[TemperatureSensor] {
    override def accept(t: PreparedStatement, u: TemperatureSensor): Unit = {
      t.setString(1, u.id)
      t.setLong(2, u.timestamp)
      t.setString(3, u.name)
      t.setDouble(4, u.temperature)
      t.setString(5, u.location)
    }
  }

}
