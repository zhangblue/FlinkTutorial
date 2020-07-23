[TOC]

# Flink Window

## 1. window类型

- 时间窗口(Time Window)
  - 滚动时间窗口(Tumbling Windows)
    - 将数据依据固定的窗口长度对数据进行切分
    - 时间对齐,窗口长度固定,没有重叠
    - 时间跨度: 左闭右开
  - 滑动时间窗口 (Sliding Windows)
    - 滑动窗口是固定窗口的更广义的一种形式, 滑动窗口由固定的窗口长度和滑动间隔组成
    - 窗口长度固定, 可以有重叠
  - 会话窗口 (Session Windows)
    - 由一系列事件组合一个置顶时间长度的timeout间隙组成, 也就是一段时间没有接收到新数据就会生成新的窗口
    - 特点: 时间无对齐
- 计数窗口(Count Window)
  - 滚动技计数窗口
  - 滑动计数窗口

## 2. window API

### 2.1 窗口分配器 (window assigner)

- window()方法接口的输入参数是一个WindowAssigner
- WindowAssigner 负责将每条输入的数据分发到正确的window中
- Flink提供了通用的WindowAssigner
  - 滚动窗口 (tumbling window)
    - `.timeWindow(Time.seconds(15))`
  - 滑动窗口 (sliding window)
    - `.timeWindow(time.seconds(15),Time.seconds(5))`
  - 会话窗口 (session window)
    - `.window(EventTimeSesionWindows.withGap(Time.minutes(10)))`
  - 全局窗口 (global window)

### 2.2 窗口函数 (window function)

- window function定一个要对窗口中手机的数据做的计算操作
- 可以分为两类
  - 增量聚合函数 (incremental aggregation functions)
    - 每条数据到来就进行计算, 保持一个简单的状态
    - ReduceFcuntion, AggregateFunction
  - 全窗口函数 (full window functions)
    - 先把窗口所有数据收集起来, 等到计算的时候会便利所有数据
    - ProcessWindowFunction

### 2.3 其他可选API

- `.trigger()` -- 触发器
  - 定义window什么时候关闭, 触发计算并输出结果
- `.evictor()` -- 移除器
  - 定义移除某些数据的逻辑
- `.allowedLateness()` -- 允许处理迟到的数据
- `sideOutputLateData()` -- 将迟到的数据放入侧输出流
- `.getSideOutput()` -- 获取侧输出流

### 2.4 Window API 概览



# Watermark

## 1. TimestampAssigner 时间分配器

### 1.1 AssignerWithPunctuatedWatermarks

- 没有时间规律，可以打断的生成watermark。

- 当数据稀疏时使用较好，每条数据都产生一个watermark

```scala
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
```



### 1.2 AssignerWithPeriodicWatermarks

- 周期性的生成watermark， 系统会周期性的将watermark插入到流中
- 默认周期是200毫秒，可以使用`env.getConfig.setAutoWatermarkInterval(100L)`方法进行设置
- 升序和乱序的处理`BoundedOutOfOrdernessTimestampExtractor` ， 都是 属于周期性watermark

- 当数据密集时使用较好， 不会每条数据都会产生一个watermark

```scala
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
```



