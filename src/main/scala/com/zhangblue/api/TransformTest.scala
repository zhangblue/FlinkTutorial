package com.zhangblue.api

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala._

/**
 * @author di.zhang
 * @date 2020/6/22
 * @time 21:21
 **/
object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行度为1
    env.setParallelism(1)

    val inputStreamFromFile: DataStream[String] = env.readTextFile("src/main/resources/sensor")

    // 1.基本转换操作
    val dataStream: DataStream[SensorReading] = inputStreamFromFile.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
    })

    // 2.分组滚动聚合
    val aggStream: DataStream[SensorReading] = dataStream
      //.keyBy(0) 按照第一个元素作为key
      //.keyBy("id") 按照SensorReading 类中的id字段作为key
      //.keyBy(data => data.id) 按照data中的id作为key
      .keyBy(new MyIDSelector()) //自定义key选择器，实现对应的getkey函数
      //.minBy("temperature") //获取当前sensor的最小值
      //      .reduce(
      //        (curRes, newData) => SensorReading(curRes.id, curRes.timestamp.max(newData.timestamp), curRes.temperature.min(newData.temperature))
      //      ) //聚合出每个sensor的最大 时间戳和最小温度值
      .reduce(new MyReduce()) //适用自定义reduceFunction函数

    // 3. 分流
    val splitStream: SplitStream[SensorReading] = dataStream.split(data => {
      if (data.temperature > 30) {
        Seq("high")
      } else {
        Seq("low")
      }
    })
    val highTempStream: DataStream[SensorReading] = splitStream.select("high")
    val lowTempStream: DataStream[SensorReading] = splitStream.select("low")
    val allTempStream: DataStream[SensorReading] = splitStream.select("high", "low")


    //4. 合流操作
    // 4.1 connect 可以将两条类型不一样的流进行合流
    val warningStream: DataStream[(String, Double)] = highTempStream.map(data => {
      (data.id, data.temperature)
    })

    val connectedStreams: ConnectedStreams[(String, Double), SensorReading] = warningStream.connect(lowTempStream)

    val resultStream: DataStream[Object] = connectedStreams.map(
      warningData => (warningData._1, warningData._2, "hight temp warning"),
      lowTemData => (lowTemData.id, "normal")
    )

    //4.2 union 能将多条类型相同的流进行合流
    val unionStream: DataStream[SensorReading] = highTempStream.union(lowTempStream, allTempStream)


    resultStream.print("result")


    //    highTempStream.print("high")
    //    lowTempStream.print("low")
    //    allTempStream.print("all")

    env.execute()

  }
}

//自定义函数类，key选择器
class MyIDSelector extends KeySelector[SensorReading, String] {
  override def getKey(in: SensorReading): String = in.id
}

//自定义reduce function
class MyReduce extends ReduceFunction[SensorReading] {
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
    SensorReading(value1.id, value1.timestamp.max(value2.timestamp), value1.temperature.min(value2.temperature))
  }
}
