package org.mj.flink.example.streaming

import scala.collection.JavaConversions.mapAsScalaMap
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{ FlinkKafkaConsumer09, FlinkKafkaProducer09 }
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.slf4j.LoggerFactory
import java.util.Properties
import org.apache.flink.streaming.util.serialization.TypeInformationKeyValueSerializationSchema
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.mj.kafka.serde.KafkaByteArrayDeserializationSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.CheckpointingMode
import org.mj.file.FileHelper
import scala.util.Random
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.mj.yaml.YamlSerDe
import org.mj.flink.config.FlinkConfiguration

/**
 * /pkg/flink/bin/flink run -c org.mj.flink.example.streaming.StreamingKafkaWordCount target/apache-flink-sample.jar
 */
object StreamingKafkaWordCount {
  private val LOG = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = kafkaConsumerProducerDemo(args)

  private def kafkaConsumerProducerDemo(args: Array[String]) = {
    //    val url = "http://api.hostip.info/get_json.php?ip=12.215.42.19"
    //    val result = scala.io.Source.fromURL(url).mkString

    // start the streaming process
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 10*1000))
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(5*1000)

    // get configuration
    val parameterTool = ParameterTool.fromArgs(args)
    val appConfig = YamlSerDe.configure.loadConfiguration[FlinkConfiguration](parameterTool.getRequired("config")).validateConfig
    LOG.info(s"Running with ${appConfig}")

    val kafkaConsumerConf = new Properties
    appConfig.kafkaConsumerConf.foreach(kv => kafkaConsumerConf.setProperty(kv._1, kv._2))
    kafkaConsumerConf.setProperty("enable.auto.commit" , "true")
    val kafkaProducerConf = new Properties
    appConfig.kafkaProducerConf.foreach(kv => kafkaProducerConf.setProperty(kv._1, kv._2))
    val kafkaReadSchema = new KafkaByteArrayDeserializationSchema
    val kafkaLogInputSource2 = new FlinkKafkaConsumer09(appConfig.inputTopicName, kafkaReadSchema, kafkaConsumerConf)
    val kafkaLogStream2 = env.addSource(kafkaLogInputSource2)
//    val kafkaLogInputSource1 = new FlinkKafkaConsumer09(appConfig.replayTopicName, kafkaReadSchema, consumerProps)
//    val kafkaLogStream1 = env.addSource(kafkaLogInputSource1)

    val kafkaLogStream = kafkaLogStream2//.union(kafkaLogStream1)

    //kafkaLogStream.keyBy(_.topic).sum(1)

    val splitStream: DataStream[String] =
      kafkaLogStream
        .map(kl => {
          val key = new String(kl.key)
          val msg = new String(kl.message)
          LOG.info(s"Consuming $kl $key $msg")
          msg
        })
        .map { msg =>
          {
            //val url = "http://api.icndb.com/jokes/random"
            //FileHelper.callRestApi(url).getBody.toString // Rest api call
            //Thread.sleep(Random.nextInt(10*1000)) // processing time
            msg
          }
        }
        .flatMap(_.split("\\s+|,|:|\""))

    splitStream.addSink(new FlinkKafkaProducer09[String](appConfig.outputTopicName, new SimpleStringSchema(), kafkaProducerConf))

    val job = env.execute("Flink-Kafka Consumer-Producer")
  }
}