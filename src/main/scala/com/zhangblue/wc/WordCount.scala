package com.zhangblue.wc

import org.apache.flink.api.scala._

//批处理word count
object WordCount {

  def main(args: Array[String]): Unit = {
    //创建一个批处理的执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
   //从文件中读取数据
    val inputPath = "src/main/resources/hello.txt"
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
