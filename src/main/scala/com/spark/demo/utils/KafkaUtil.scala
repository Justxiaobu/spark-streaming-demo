package com.spark.demo.utils

import org.apache.kafka.common.serialization.StringDeserializer

/**
  * author sunbin
  * date 2018-05-24 10:42
  * description kafka工具类
  */
object KafkaUtil {
  val KAFKA_BROKERS=LoadConfs.getProperty("kafka.brokers")
  val GROUP_ID=LoadConfs.getProperty("group.id")

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> KAFKA_BROKERS,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> GROUP_ID,
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topic1=LoadConfs.getProperty("kafka.topic1")

}
