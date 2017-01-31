package org.mj.flink.config

import scala.beans.BeanProperty
import org.apache.commons.lang.builder.ToStringBuilder
import org.apache.commons.lang.builder.ToStringStyle

abstract class SamplerConfig extends Serializable {
  @BeanProperty var appName: String = "FlinkApp"
  @BeanProperty var sampleCount: java.lang.Integer = 10
  @BeanProperty var flinkConf: java.util.Map[String, String] =_
  @BeanProperty var outputFileDirectory: String = "/tmp/"
  @BeanProperty var keepRunning = false
  @BeanProperty var buffer: Double = 100.0

  override def toString(): String = ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE)
}