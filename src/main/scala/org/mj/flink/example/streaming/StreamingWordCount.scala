package org.mj.flink.example.streaming

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{ FlinkKafkaConsumer09, FlinkKafkaProducer09 }
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.slf4j.LoggerFactory
import java.util.Properties

/**
 * /pkg/flink/bin/flink run -c org.mj.flink.example.streaming.StreamingWordCount target/apache-flink-sample.jar
 */
object StreamingWordCount {
  private val LOG = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = kafkaConsumerProducerDemo(args)

  private def kafkaConsumerProducerDemo(args: Array[String]) = {
    // set the consumer and producer configurations
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "cg")
    properties.setProperty("auto.offset.reset", "earliest");

    val consumerProps = properties
    val producerProps = properties

    // start the streaming process
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env
      .addSource(new FlinkKafkaConsumer09[String]("testIn", new SimpleStringSchema(), consumerProps))

    val splitStream = stream
      .flatMap(l => l.split("\\s+|,|:|\""))

    splitStream.addSink(new FlinkKafkaProducer09[String]("testOut", new SimpleStringSchema(), producerProps))
    splitStream.print

    val job = env.execute("Flink-Kafka Consumer-Producer")
  }
}