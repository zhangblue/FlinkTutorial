package com.zhangblue.source.self

import java.util.concurrent.TimeUnit


import com.zhangblue.entity.TemperatureSensor
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.io.Source
import scala.util.Random

/**
 * 自定义source
 *
 * 读取src/main/resources/temperature-sensor.txt文件，每1秒随机取文件中的一行内容，封装成自定义类，作为source发送给stream
 */
object SelfAsSourceDemo {
  def main(args: Array[String]): Unit = {
    //1. 环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //2. 自定义SourceFunction接口的子类
    //2.1 方式1： 匿名内部类
    //2.2 方式2： 成员内部类
    val sleepTime = 1
    val logFilePath = "src/main/resources/temperature-sensor.txt"
    val mySrc = new MySource(sleepTime, logFilePath)
    //3. 从自定义的Source中读取数据，计算，显示结果
    env.addSource(mySrc).print("自定义source结果 : ")
    //4. 启动
    env.execute("SelfAsSourceDemo")
  }

  /**
   * 自定义source实现类
   *
   * @param sleepTimeSec 休息时间
   * @param logFilePath  日志文件的路径
   */
  private class MySource(sleepTimeSec: Int, logFilePath: String) extends SourceFunction[TemperatureSensor] {

    /**
     * 标识值：true->继续发送数据 false->停止发送数据
     */
    private var flg = true

    override def run(ctx: SourceFunction.SourceContext[TemperatureSensor]): Unit = {
      //1. 读取文件, 封装成TemperatureSensor的实例
      val lst: List[TemperatureSensor] = Source
        .fromFile(logFilePath).getLines().toList.map(lineData => {
        val arr = lineData.split(",")
        val id = arr(0).trim
        val timestamp = arr(1).trim.toLong
        val name = arr(2).trim
        val temperature = arr(3).trim.toDouble
        val location = arr(4).trim
        TemperatureSensor(id, timestamp, name, temperature, location)
      })
      //2. 如果没有cancel，通过循环来模拟每间隔1秒钟向Source发送一条数据。
      while (flg) {
        val randomIndex = Random.nextInt(lst.size)
        val randomInfo = lst(randomIndex)
        ctx.collect(randomInfo)
        TimeUnit.SECONDS.sleep(sleepTimeSec)
      }
    }

    override def cancel(): Unit = flg = false
  }

}
