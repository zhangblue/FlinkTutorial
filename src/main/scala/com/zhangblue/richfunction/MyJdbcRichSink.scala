package com.zhangblue.richfunction

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.zhangblue.entity.TemperatureSensor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
 * @author di.zhang
 * @date 2020/7/23
 * @time 14:28
 **/
class MyJdbcRichSink extends RichSinkFunction[TemperatureSensor] {
  //定义sql链接，以及预编译语句
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/zhangblue_test?characterEncoding=utf8&useSSL=true", "root", "12345678")
    insertStmt = conn.prepareStatement("insert into temperature_sensor (id,timestamp,name,temperature,location) values (?,?,?,?,?)")
    updateStmt = conn.prepareStatement("update temperature_sensor set temperature=? where id=?")
  }

  override def invoke(value: TemperatureSensor, context: SinkFunction.Context[_]): Unit = {
    //执行更新语句
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()

    //如果刚才没有更新数据，则执行插入操作
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setLong(2, value.timestamp)
      insertStmt.setString(3, value.name)
      insertStmt.setDouble(4, value.temperature)
      insertStmt.setString(5, value.location)
      insertStmt.execute()
    }
  }


  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}
