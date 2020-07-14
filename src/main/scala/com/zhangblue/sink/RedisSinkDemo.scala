package com.zhangblue.sink

import com.zhangblue.entity.TemperatureSensor
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @author di.zhang
 * @date 2020/7/14
 * @time 23:52
 **/
object RedisSinkDemo {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val inputStream: DataStream[String] = env.readTextFile("src/main/resources/temperature-sensor.txt")


    val dataStream: DataStream[TemperatureSensor] = inputStream.map(new MyMapFunction())

    //定义一个redis 的配置陪
    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build()


    dataStream.addSink(new RedisSink[TemperatureSensor](conf, new MyRedisMapper))

    env.execute("redis sink demo")
  }


  private class MyMapFunction extends MapFunction[String, TemperatureSensor] {
    override def map(value: String): TemperatureSensor = {
      val splitData = value.split(",")
      TemperatureSensor(splitData(0), splitData(1).toLong, splitData(2), splitData(3).toDouble, splitData(4))
    }
  }

  private class MyRedisMapper extends RedisMapper[TemperatureSensor] {
    //定义保存数据到redis的命令
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET, "temperatureSensor_tmp")
    }

    override def getKeyFromData(data: TemperatureSensor): String = {
      data.id
    }

    override def getValueFromData(data: TemperatureSensor): String = {
      data.temperature.toString
    }
  }

}
