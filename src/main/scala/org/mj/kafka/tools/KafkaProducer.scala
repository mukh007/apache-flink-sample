package org.mj.kafka.tools

import java.util.concurrent.Future

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.joda.time.DateTime
import org.mj.json.JsonSerDe
import org.slf4j.LoggerFactory
import scala.util.Random

object KafkaProducer {
  val LOG = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {
    val props = new java.util.Properties
    props.put("bootstrap.servers", "localhost:9092")
    props.put("client.id", "KafkaProducer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val topicName2 = "testIn2"
    val topicName1 = "testIn1"
    val producer = new KafkaProducer[Integer, String](props)

    val sleepTime = args(0).toLong
    var i = 1
    while (i>0) {
      val msg2 = s"T2val-$i-${System.currentTimeMillis}"
      val record2 = new ProducerRecord[Integer, String](topicName2, Random.nextInt(10), msg2)
      val metaF2: Future[RecordMetadata] = producer.send(record2)
      val meta2 = metaF2.get() // blocking!
      LOG.info(s"Produced $msg2 to ${meta2.topic()}-${meta2.partition()}:${meta2.offset()}")
      val msg1 = s"T1val-$i-${System.currentTimeMillis}"
      val record1 = new ProducerRecord[Integer, String](topicName1, Random.nextInt(10), msg1)
      val metaF1: Future[RecordMetadata] = producer.send(record1)
      val meta1 = metaF1.get() // blocking!
      LOG.info(s"Produced $msg1 to ${meta1.topic()}-${meta1.partition()}:${meta1.offset()}")
      Thread.sleep(sleepTime)
      i = i + 1
    }
    producer.close()
  }
}