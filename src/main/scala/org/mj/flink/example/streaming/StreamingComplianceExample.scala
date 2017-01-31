package org.mj.flink.example.streaming

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.mj.flink.model.Event
import org.mj.flink.model.EventType
import org.mj.flink.sink.EventSink
import org.mj.flink.source.EventGenerator
import org.slf4j.LoggerFactory
import org.apache.flink.runtime.state.filesystem.FsStateBackend

/**
 * /pkg/flink/bin/flink run -c org.mj.flink.example.streaming.StreamingComplianceExample target/apache-flink-sample.jar
 */
object StreamingComplianceExample {
  private val LOG = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org.mj").setLevel(Level.INFO)

  val LOG_AFTER_MSGS = 100//00//00

  def main(args: Array[String]): Unit = consumerProducerDemo(args)

  private def consumerProducerDemo(conf: Array[String]) = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(new FsStateBackend("file:/tmp/flink/checkpoints"))
    env.enableCheckpointing(10000)

    val stream = env
      .addSource(new EventGenerator(10))

    val splitStream = stream
      .flatMap { event => event }

    splitStream.addSink(new EventSink[Event[String, Integer, EventType.Value]])

    val job = env.execute("Flink-Kafka Consumer-Producer")
  }

  case class WordWithCount(word: String, count: Long)
}