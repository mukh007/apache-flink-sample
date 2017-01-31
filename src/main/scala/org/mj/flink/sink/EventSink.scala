package org.mj.flink.sink

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.slf4j.LoggerFactory
import org.mj.json.JsonSerDe
import org.mj.flink.model.Event
import org.mj.flink.model.EventType
import org.mj.flink.example.streaming.StreamingAggregaeExample

class EventSink[T] extends SinkFunction[T] {
  private val LOG = LoggerFactory.getLogger(getClass)

  override def invoke(next: T) = {
    val item = next.asInstanceOf[Event[String, Integer, EventType.Value]]
    if(item.value%StreamingAggregaeExample.LOG_AFTER_MSGS == 0) {
    	LOG.info("  Consumed {}", next)
    }
//    LOG.info("Consumed {}", JsonSerDe.serialize(next))
    Thread.sleep(50)
  }
}