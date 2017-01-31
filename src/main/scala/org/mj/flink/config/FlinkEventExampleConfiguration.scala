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
class SparkHBaseSamplerConfiguration extends SamplerConfig with Serializable {
  @BeanProperty var hbaseTableName: String = "canonical_message"
  @BeanProperty var hbaseZkQuorum: String = "localhost"
  @BeanProperty var hbaseZkPort: String = "2181"
  @BeanProperty var startHour: Date = _
  @BeanProperty var endHour: Date = _
  @BeanProperty var positiveFilePathName: String = "positivefilepath.txt"
  @BeanProperty var negetiveFilePathName: String = "negetivefilepath.txt"
  @BeanProperty var filterFilePathName: String = "filterfilepath.txt"
  @BeanProperty var hbaseConf: java.util.Map[String, String] = _

  override def toString(): String = ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE)
}