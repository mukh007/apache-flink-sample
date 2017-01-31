package org.mj.kafka.serde

import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema
import com.oracle.ci.model.KafkaLog
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor

class KafkaByteArrayDeserializationSchema extends KeyedDeserializationSchema[KafkaLog[Array[Byte], Array[Byte]]] {
  override def deserialize(messageKey: Array[Byte], message: Array[Byte], topic: String, partition: Int, offset: Long): KafkaLog[Array[Byte], Array[Byte]] = {
    new KafkaLog(topic, partition, offset, messageKey, message)
  }

  override def isEndOfStream(kafkalog: KafkaLog[Array[Byte], Array[Byte]]): Boolean = {
    false
  }

  override def getProducedType: TypeInformation[KafkaLog[Array[Byte], Array[Byte]]] = {
    TypeExtractor.getForClass(classOf[KafkaLog[Array[Byte], Array[Byte]]])
  }
}