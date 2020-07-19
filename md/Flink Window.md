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