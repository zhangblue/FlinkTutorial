package com.zhangblue.function

import com.zhangblue.entity.TemperatureSensor
import org.apache.flink.api.common.functions.MapFunction

/**
 * @author di.zhang
 * @date 2020/7/23
 * @time 11:48
 **/
class MyMapFunction extends MapFunction[String, TemperatureSensor] {
  override def map(value: String): TemperatureSensor = {
    val split = value.split(",");
    if (split.length != 5) {
      None.get
    } else {
      TemperatureSensor(split(0).trim, split(1).trim.toLong, split(2).trim, split(3).trim.toDouble, split(4).trim)
    }
  }
}
