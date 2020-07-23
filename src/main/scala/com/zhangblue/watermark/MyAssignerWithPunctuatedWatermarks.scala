package com.zhangblue.watermark

import com.zhangblue.entity.TemperatureSensor
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
 * 自定义连续生成watermark类， 此种类型的watermark分配器会每条数据都会产生一个watermark
 */
class MyAssignerWithPunctuatedWatermarks(lateness: Long) extends AssignerWithPunctuatedWatermarks[TemperatureSensor] {

  //此方法每条数据都会调用， 并生成一个watermark
  /**
   *
   * @param lastElement        当前的这条数据
   * @param extractedTimestamp 从当前的这条数据中提取的时间戳
   * @return
   */
  override def checkAndGetNextWatermark(lastElement: TemperatureSensor, extractedTimestamp: Long): Watermark = {
    //此处可以根据消息的类型，判断是否生成watermark
    if (lastElement.id == "sensor_1") {
      new Watermark(extractedTimestamp - lateness)
    } else {
      null
    }
  }

  /**
   * 时间戳提取器
   *
   * @param element                  当前的这条数据
   * @param previousElementTimestamp 上条数据的时间
   * @return
   */
  override def extractTimestamp(element: TemperatureSensor, previousElementTimestamp: Long): Long = {
    element.timestamp
  }
}
