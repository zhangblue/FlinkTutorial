package com.zhangblue.richfunction

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

/**
 * @author di.zhang
 * @date 2020/7/13
 * @time 23:51
 **/
object MyRichMapFunctionDemo {

  def main(args: Array[String]): Unit = {

  }

  private class MyRichMapFunction extends RichMapFunction[String, (String, Long)] {

    override def open(parameters: Configuration): Unit = super.open(parameters)

    override def map(value: String): (String, Long) = ???

    override def close(): Unit = super.close()

  }

}
