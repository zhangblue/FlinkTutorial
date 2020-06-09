# Flink Demo

工程使用
- JDK 1.8 
- Scala 2.11.12

## 1. Word Count
### 1.1 批处理
```scala
package com.zhangblue.wc

import org.apache.flink.api.scala._

//批处理word count
object WordCount {

  def main(args: Array[String]): Unit = {
    //创建一个批处理的执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
   //从文件中读取数据
    val inputPath = "D:\\software\\workspace\\FlinkTutorial\\src\\main\\resources\\hello.txt"
    val inputDataSet = env.readTextFile(inputPath)

    //分词之后做count
   val wordCountDataSet = inputDataSet.flatMap(_.split(" "))
     .map((_,1))
     .groupBy(0)
     .sum(1)

    //打印输出
    wordCountDataSet.print()
  }
}
```

### 1.2 流式处理
```scala
package com.zhangblue.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * 流式处理word count
 */
object StreamWordCount {

  def main(args: Array[String]): Unit = {

    //通过参数进行传递. 参数传递方式：--host localhost --port 7777
    val parameter = ParameterTool.fromArgs(args)
    val host:String  = parameter.get("host")
    val port:Int = parameter.getInt("port")


    //创建一个流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //接收socket数据流
    val testDataStream = env.socketTextStream(host,port)

    //逐一读取数据
   val wordCountDataStream = testDataStream.flatMap(_.split(" "))
     .filter(_.nonEmpty)
     .map((_,1))
     .keyBy(0)
     .sum(1)

    //打印输出
    wordCountDataStream.print()

    //执行任务
    env.execute("Stream WordCount Example")
  }
}
```