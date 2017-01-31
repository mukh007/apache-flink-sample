package org.mj.flink.model

import org.apache.commons.lang.builder.ToStringBuilder
import org.apache.commons.lang.builder.ToStringStyle

case class ComplianceEvent (
    val id: String,
    val key: String,
    val source_name: String,
    val compliance_action: String,
    val source_generated_id: String,
    val author_source_generated_id: String,
    val data_source_context: String,
    val posted_at: Long
) {
  override def toString: String = ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE)
}