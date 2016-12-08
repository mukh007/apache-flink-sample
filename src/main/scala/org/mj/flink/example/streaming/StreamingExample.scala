package org.mj.flink.example.streaming

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{ FlinkKafkaConsumer09, FlinkKafkaProducer09 }
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory
import java.util.Properties
import org.mj.flink.source.SimpleStringGenerator
import org.mj.flink.sink.SimpleSink
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * /pkg/flink/bin/flink run -c org.mj.flink.example.streaming.StreamingExample target/apache-flink-sample.jar
 */
object StreamingExample {
  private val LOG = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org.mj").setLevel(Level.DEBUG)

  def main(args: Array[String]): Unit = consumerProducerDemo(args)

  private def consumerProducerDemo(args: Array[String]) = {
    // start the streaming process
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env
      .addSource(new SimpleStringGenerator(2))

    val splitStream = stream
      .flatMap { l => l.split("\\s+|,|:|\"") }

    val windowCounts = splitStream.map { word => WordWithCount(word, 1) }
      .keyBy(_.word)
      .timeWindow(Time.seconds(10), Time.seconds(1))
      .sum("count")

    windowCounts.addSink(new SimpleSink[WordWithCount])
    windowCounts.print

    val job = env.execute("Flink-Kafka Consumer-Producer")
  }

  case class WordWithCount(word: String, count: Long)
}