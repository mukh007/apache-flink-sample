package org.mj.flink.config

import java.util.Date

import scala.beans.BeanProperty

import org.apache.commons.lang.builder.ToStringBuilder
import org.apache.commons.lang.builder.ToStringStyle

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonInclude

/**
 * Configuration for SparkHBaseSampler
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
class FlinkConfiguration extends Serializable {
  @BeanProperty var inputTopicName: String = "testIn1"
  @BeanProperty var replayTopicName: String = "testIn2"
  @BeanProperty var outputTopicName: String = "testOut"
  @BeanProperty var kafkaConsumerConf: java.util.Map[String, String] = _
  @BeanProperty var kafkaProducerConf: java.util.Map[String, String] = _

  override def toString(): String = ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE)

  def validateConfig = {
    this
  }
}