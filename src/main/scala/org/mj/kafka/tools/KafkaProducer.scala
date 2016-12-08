package org.mj.kafka.tools

import java.util.concurrent.Future

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.joda.time.DateTime
import org.mj.json.JsonSerDe
import org.slf4j.LoggerFactory

object KafkaProducerTool {
  val LOG = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {
    val props = new java.util.Properties
    props.put("bootstrap.servers", "localhost:9092")
    props.put("client.id", "KafkaProducer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val topicName = "testIn"
    val producer = new KafkaProducer[Integer, String](props)

    val sleepTime = args(0).toLong
    while (true) {
      val record = new ProducerRecord[Integer, String](topicName, 1, JsonSerDe.serialize(DateTime.now()))
      val metaF: Future[RecordMetadata] = producer.send(record)
      val meta = metaF.get() // blocking!
      LOG.info(s"Produced msg to ${meta.topic()}-${meta.partition()}:${meta.offset()}")
      Thread.sleep(sleepTime)
    }
    producer.close()
  }
}