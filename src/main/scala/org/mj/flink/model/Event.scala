package org.mj.flink.model

import org.apache.commons.lang.builder.ToStringBuilder
import org.apache.commons.lang.builder.ToStringStyle
import scala.beans.BeanProperty
import org.codehaus.jackson.annotate.JsonProperty
import org.joda.time.DateTime
import org.mj.json.JsonSerDe

case class Event[K, V, T] (
		@BeanProperty @JsonProperty("event_time") val eventTime: Long,
    @BeanProperty @JsonProperty("key") val key: K,
    @BeanProperty @JsonProperty("value") val value: V,
    @BeanProperty @JsonProperty("event_type") val eventType: T
) {
  override def toString: String = ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE)
}