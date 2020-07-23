package com.zhangblue.watermark

import com.zhangblue.entity.TemperatureSensor
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
 * 自定义定期生成watermark 类, 此种类型的watermark分配器会每隔200毫秒生成一个watermark
 */
class MyAssignerWithPeriodicWatermarks(lateness: Long) extends AssignerWithPeriodicWatermarks[TemperatureSensor] {

  //存放最大时间戳
  var maxTimeStamp: Long = Long.MinValue + maxTimeStamp

  /**
   * 计算watermark， 次方法会每隔200毫秒触发一次， 生成一个watermark
   *
   * @return
   */
  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTimeStamp - lateness)
  }

  /**
   * 数据时间戳提取器，次方法每条数据都会执行
   *
   * @param element                  当前的这条数据
   * @param previousElementTimestamp 上条数据的时间
   * @return
   */
  override def extractTimestamp(element: TemperatureSensor, previousElementTimestamp: Long): Long = {
    //计算最大时间戳
    maxTimeStamp = maxTimeStamp.max(element.timestamp)
    //返回当前数据的时间戳
    element.timestamp
  }
}
