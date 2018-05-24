package com.spark.demo.process

import com.spark.demo.utils.{KafkaUtil, LoadConfs, ZkUtil}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.slf4j.LoggerFactory


/**
  * author sunbin
  * date 2018-05-24 10:42
  * description 
  */
object StreamingDemo {

  val zk = ZkUtil

  def main(args: Array[String]): Unit = {
    /**
      * 三板斧
      */
    val conf = new SparkConf().setAppName("StreamingDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(LoadConfs.getInteger("streaming.seconds").toInt))
    val logger = LoggerFactory.getLogger(getClass)
    /**
      * 判断zk中是否有偏移量
      * 如果有则使用，如果没有则使用不带偏移量的计算
      * 保存计算完的偏移量
      */

    val topic = KafkaUtil.topic1
    val topics = Array(topic)
    val group = KafkaUtil.GROUP_ID
    val stream= if(zk.znodeIsExists(s"${topic}offset")){
      val nor = zk.znodeDataGet(s"${topic}offset")
      //创建以topic，分区为k 偏移度为v的map
      val newOffset = Map(new TopicPartition(nor(0).toString, nor(1).toInt) -> nor(2).toLong)
      logger.info(s"[ StreamingDemo ] topic ${nor(0).toString}")
      logger.info(s"[ StreamingDemo ] Partition ${nor(1).toInt}")
      logger.info(s"[ StreamingDemo ] offset ${nor(2).toLong}")
      logger.info(s"[ StreamingDemo ] zk中取出来的kafka偏移量★★★ $newOffset")
      KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, KafkaUtil.kafkaParams, newOffset))
    }else{
      zk.znodeCreate(s"${topic}offset",s"$topic,0,0")
      val nor = zk.znodeDataGet(s"${topic}offset")
      //创建以topic，分区为k 偏移度为v的map
      val newOffset = Map(new TopicPartition(nor(0).toString, nor(1).toInt) -> nor(2).toLong)
      logger.info(s"[ StreamingDemo ] --------------------------------------------------------------------")
      logger.info(s"[ StreamingDemo ] topic ${nor(0).toString}")
      logger.info(s"[ StreamingDemo ] Partition ${nor(1).toInt}")
      logger.info(s"[ StreamingDemo ] offset ${nor(2).toLong}")
      logger.info(s"[ StreamingDemo ] zk中取出来的kafka偏移量★★★ $newOffset")
      logger.info(s"[ StreamingDemo ] --------------------------------------------------------------------")
      KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, KafkaUtil.kafkaParams, newOffset))
    }

    //业务计算
    val lines =stream.map(_.value())
    lines.count().print()
//    val result = lines


    /**
      * 保存偏移量
      */
    stream.foreachRDD{
      rdd =>
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition {
          iter =>
            val off: OffsetRange = offsetRanges(TaskContext.get.partitionId)
            logger.info(s"[ StreamingDemo ] --------------------------------------------------------------------")
            logger.info(s"[ StreamingDemo ]  topic: ${off.topic}")
            logger.info(s"[ StreamingDemo ]  partition: ${off.partition} ")
            logger.info(s"[ StreamingDemo ]  fromOffset 开始偏移量: ${off.fromOffset} ")
            logger.info(s"[ StreamingDemo ]  untilOffset 结束偏移量: ${off.untilOffset} ")
            logger.info(s"[ StreamingDemo ] --------------------------------------------------------------------")
            // 写zookeeper
            zk.offsetWork(s"${off.topic}offset", s"${off.topic},${off.partition},${off.untilOffset}")
        }
    }

    //业务入库


    //启动/关闭
    ssc.start()
    ssc.awaitTermination()


  }
}
