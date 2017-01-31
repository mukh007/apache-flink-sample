package com.oracle.ci.model

/**
 * Model to hold kafka results
 */
case class KafkaLog[K, V] (
    val topic: String, 
    val partition: Int, 
    val offset: Long,
    val key: K,
    val message: V
    ) {
	override def toString(): String = {
    s"$topic-$partition:$offset"
	}
}