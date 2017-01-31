package org.mj.flink.source

import java.util.ArrayList

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.mj.flink.model.Event
import org.mj.flink.model.EventType
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import scala.util.Random
import org.mj.flink.example.streaming.StreamingAggregaeExample
import org.apache.flink.runtime.state.CheckpointListener

class EventGenerator(
    val size: Int
)  extends SourceFunction[List[Event[String, Integer, EventType.Value]]]
{
  private val LOG = LoggerFactory.getLogger(getClass)
  var isRunning = true;
  val rand = new Random

  override def run(ctx: SourceContext[List[Event[String, Integer, EventType.Value]]]) = {
    val eventTypes = EventType.values.toList
    var value: Integer = 0

    while(isRunning) {
      val eventList = new ArrayList[Event[String, Integer, EventType.Value]]()
      val currentTime = DateTime.now
      val timeFluctuation = 1*60*60*1000 // millis

      (0 to size).foreach { i => {
        val timeInMillis = currentTime.minusMillis(rand.nextInt(timeFluctuation)).getMillis
        val eventType = eventTypes(rand.nextInt(eventTypes.size))
        value += 1
        val event = new Event(timeInMillis, "key", value, eventType)
        eventList.add(event)
        if(value%StreamingAggregaeExample.LOG_AFTER_MSGS == 0) {
          LOG.info("Producing {}", event)
        }
      }}
      LOG.debug("Producing {} events", eventList.size())
      ctx.collect(eventList.toList)
      Thread.sleep(50)
    }
  }

  override def cancel() = {
    isRunning = false
  }
}