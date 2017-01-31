package org.mj.flink.compliance.JsonTest

import scala.collection.JavaConverters._
import scala.collection.JavaConversions.asScalaBuffer

import scala.io.Source
import org.mj.json.JsonSerDe
import org.mj.flink.model.ComplianceEvent

object ComplianceEventTest {
  def main(args: Array[String]): Unit = {
    val jsonString = Source.fromFile("/Users/mujha/Desktop/compliance-messages-staging-0/129534").mkString
    val complianceEvents = JsonSerDe.deserialize[List[ComplianceEvent]](jsonString)
    complianceEvents.foreach { e => println(e)}
  }
}