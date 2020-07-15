
import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zhangblue.entity.TemperatureSensor
import org.apache.flink.api.common.functions.{MapFunction, RuntimeContext}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.{ElasticsearchSink, RestClientFactory}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink.Builder
import org.apache.http.{Header, HeaderElement, HttpHost}
import org.elasticsearch.client.{Requests, RestClientBuilder}

/**
 * @author di.zhang
 * @date 2020/7/15
 * @time 19:36
 **/
object EsSinkDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputData: DataStream[String] = env.readTextFile("src/main/resources/temperature-sensor.txt")

    val dataStream: DataStream[TemperatureSensor] = inputData.map(new MyMapFunction)


    val httpHosts: util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("172.16.36.123", 9200, "http"))


    val esSinkBuilder: Builder[TemperatureSensor] = new ElasticsearchSink.Builder[TemperatureSensor](httpHosts, new MyElasticsearchSinkFunction)
    esSinkBuilder.setBulkFlushMaxActions(1)

    dataStream.addSink(esSinkBuilder.build())

    env.execute("sink to es demo")
  }


  private class MyMapFunction extends MapFunction[String, TemperatureSensor] {
    override def map(value: String): TemperatureSensor = {
      val splitData = value.split(",")
      TemperatureSensor(splitData(0), splitData(1).toLong, splitData(2), splitData(3).toDouble, splitData(4))
    }
  }

  private class MyElasticsearchSinkFunction extends ElasticsearchSinkFunction[TemperatureSensor] {
    override def process(t: TemperatureSensor, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
      val source: util.HashMap[String, String] = new util.HashMap[String, String]()
      source.put("id", t.id)
      source.put("name", t.name)
      source.put("location", t.location)
      source.put("temperature", t.temperature.toString)
      source.put("timestamp", t.timestamp.toString)

      val requests = Requests.indexRequest().index("temperature_sensor").`type`("test").source(source)
      requestIndexer.add(requests)
    }
  }

}
