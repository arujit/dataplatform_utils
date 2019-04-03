package com.goibibo.dp.utils

import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Deserializer
import org.apache.spark.streaming.kafka010._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Future}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.concurrent.ExecutionContext.Implicits.global
import org.codehaus.jackson.map.deser.std.StringDeserializer
import org.slf4j.LoggerFactory
import reflect.ClassTag
import scala.collection.JavaConverters._
import scala.util.Try

object SparkKafkaUtils09 {
  val READ_FROM_EARLIEST = "earliest"
  val READ_FROM_LATEST = "latest"
  val READ_FROM_COMMITTED = "committed"
  val BOOTSTRAP_SERVERS = "bootstrap.servers"

  type KOffsets = Map[TopicPartition, Long]
  private val logger = LoggerFactory.getLogger(SparkKafkaUtils09.getClass)

  private def createOffsetRange(kafkaBrokers: String, topics: Seq[String],
                                consumerGroup: String, maxMessagesPerPartition: Option[Int], 
                                readFrom: String = READ_FROM_COMMITTED): (Seq[OffsetRange], KOffsets, Boolean) = {

    var isReadRequired = false
    val kafkaConfig = KfUtils09.createKafkaConfig(kafkaBrokers, consumerGroup)
    val topicsNames = topics.asJava
    val earliestOffsets = KfUtils09.getEarliestOffsets(topicsNames, kafkaConfig).get
    val latestOffsets = KfUtils09.getLatestOffsets(topicsNames, kafkaConfig).get
    val committedOffsets = KfUtils09.getCommittedOffsets(topicsNames, kafkaConfig).get
    val fromOffsets =
      if (READ_FROM_EARLIEST.equals(readFrom)) earliestOffsets
      else if (READ_FROM_LATEST.equals(readFrom)) latestOffsets
      else committedOffsets
    val offsetRanges = latestOffsets.toList.map((pairTopicPartitionAndOffset) => {
      val (tp, untilOffset) = pairTopicPartitionAndOffset
      val totalMessagesInPartition = untilOffset - fromOffsets(tp)
      logger.info(s"${tp.topic} ${tp.partition} earliestOffsets =  $earliestOffsets committedOffsets = ${committedOffsets(tp)} fromOffsets = ${fromOffsets(tp)} untilOffset = $untilOffset")
      logger.info(s"${tp.topic} ${tp.partition} totalMessagesInPartition = $totalMessagesInPartition ")
      val newUntilOffset = if (maxMessagesPerPartition.isDefined) {
        if (totalMessagesInPartition > maxMessagesPerPartition.get) {
          logger.info(s"${tp.topic} ${tp.partition} totalMessagesInPartition = $totalMessagesInPartition higher than maxMessagesPerPartition = $maxMessagesPerPartition")
          val newUntilOffset = fromOffsets(tp) + maxMessagesPerPartition.get
          logger.info(s"${tp.topic} ${tp.partition} new untilOffset = $newUntilOffset")
          newUntilOffset
        } else {
          untilOffset
        }
      } else {
        untilOffset
      }
      if (newUntilOffset > fromOffsets(tp)) {
        isReadRequired = true
      }
      OffsetRange.create(tp.topic, tp.partition, fromOffsets(tp), newUntilOffset)
    })
    (offsetRanges, latestOffsets, isReadRequired)
  }

  /**
    * This method gives Rdd of key and value for a topic. Key and value both would be String.
    *
    * @param kafkaBrokers - kafka broker ip with port
    * @param topics - all the topics from which data is to be fetched
    * @param consumerGroup - group id for the consumer
    * @param sc - spark context
    * @param maxMessagesPerPartition - maximum number of messages to be fetched
    * @return
    */
  def createRdd(kafkaBrokers: String, topics: Seq[String], consumerGroup: String,
                sc: SparkContext, maxMessagesPerPartition: Option[Int] = None): (RDD[(String, String)], KOffsets) = {
    val (offsets, latestOffsets, isReadRequired) = createOffsetRange(kafkaBrokers, topics, consumerGroup, maxMessagesPerPartition)
    val kafkaParams = Map(BOOTSTRAP_SERVERS -> kafkaBrokers.asInstanceOf[Object],
    "key.deserializer" -> classOf[StringDeserializer].getName.asInstanceOf[Object],
    "value.deserializer" -> classOf[StringDeserializer].getName.asInstanceOf[Object]).asJava
    val rddKafkaC =
      if (isReadRequired) KafkaUtils.createRDD[String, String](sc, kafkaParams, offsets.toArray, LocationStrategies.PreferConsistent)
      else sc.emptyRDD[ConsumerRecord[String, String]]
    val rddKafka = rddKafkaC.map(c => (c.key, c.value))
    (rddKafka, latestOffsets)
  }

  /**
    * This method gives Rdd of type K and V for key and value respectively for a topic.
    *
    * @param kafkaBrokers - kafka broker ip with port
    * @param topics - all the topics from which data is to be fetched
    * @param consumerGroup - group id for the consumer
    * @param sc - spark context
    * @param maxMessagesPerPartition - maximum number of messages to be fetched
    * @tparam K - type of Key
    * @tparam V - type of value
    * @tparam KD - Deserializer for key
    * @tparam VD - Deserializer for value
    * @return
    */
  def createRddG[K:ClassTag, V:ClassTag, KD <: Deserializer[K]:ClassTag, VD <: Deserializer[V]:ClassTag]
  (kafkaBrokers: String, topics: Seq[String], consumerGroup: String,
   sc: SparkContext, maxMessagesPerPartition: Option[Int] = None): (RDD[(K, V)], KOffsets) =
  {
    val (offsets, latestOffsets, isReadRequired) = createOffsetRange(kafkaBrokers, topics, consumerGroup, maxMessagesPerPartition)
    val kafkaParams = Map("bootstrap.servers" -> kafkaBrokers.asInstanceOf[Object],
      "key.deserializer" ->  implicitly[ClassTag[KD]].runtimeClass.getName.asInstanceOf[Object],
      "value.deserializer" -> implicitly[ClassTag[VD]].runtimeClass.getName.asInstanceOf[Object]).asJava
    val rddKafkaC =
      if(isReadRequired) KafkaUtils.createRDD[K, V](sc, kafkaParams, offsets.toArray, LocationStrategies.PreferConsistent)
      else sc.emptyRDD[ConsumerRecord[K,V]]
    val rddKafka = rddKafkaC.map(c => (c.key, c.value))
    (rddKafka, latestOffsets)
  }

  /**
    * This method gives Rdd of ConsumerRecord for a topic.
    *
    * @param kafkaParams - kafka params
    * @param topics - all the topics from which data is to be fetched
    * @param consumerGroup - group id for the consumer
    * @param sc - spark context
    * @param maxMessagesPerPartition - maximum number of messages to be fetched
    * @tparam K - type of Key
    * @tparam V - type of value
    * @return
    */
  def createRddF[K: ClassTag, V: ClassTag](
                                            kafkaParams: Map[String,Object],
                                            topics: Seq[String],
                                            consumerGroup: String,
                                            sc: SparkContext,
                                            maxMessagesPerPartition: Option[Int] = None,
                                            readFrom: String):
  (RDD[ConsumerRecord[K, V]], KOffsets, Seq[OffsetRange]) = {

    val (offsets, latestOffsets, isReadRequired) = createOffsetRange(kafkaParams(BOOTSTRAP_SERVERS).asInstanceOf[String], topics, consumerGroup, maxMessagesPerPartition, readFrom)
    val rddKafka =
      if(isReadRequired) KafkaUtils.createRDD[K, V](sc, kafkaParams.asJava, offsets.toArray, LocationStrategies.PreferConsistent)
      else sc.emptyRDD[ConsumerRecord[K,V]]
    (rddKafka, latestOffsets, offsets)
  }

  /**
    * This function commits offsets to kafka with a timeout.
    *
    * @param kafkaBrokers    kafka broker ips with port
    * @param consumerGroup   Consumer group name
    * @param offsetsToCommit offsets which are to be committed
    * @param timeout         timeout for the commit in milliseconds
    * @return
    */
  def commitOffsetsTimeout(kafkaBrokers: String, consumerGroup: String, offsetsToCommit: KOffsets, timeout: Long): Boolean = {
    var success = false
    val commitOffsetF = Future[Boolean] {
      commitOffsets(kafkaBrokers, consumerGroup, offsetsToCommit)
    }
    val commitOffsetWithTimeoutF = FutureUtils.futureWithTimeout(commitOffsetF, FiniteDuration.apply(timeout, TimeUnit.MILLISECONDS)).map {
      result => if (result) success = true
    }
    Await.ready(commitOffsetWithTimeoutF, FiniteDuration.apply(timeout, TimeUnit.MILLISECONDS))
    success
  }

  def commitOffsets(kafkaBrokers: String, consumerGroup: String, offsetsToCommit: KOffsets): Boolean = {
    Try {
      val kafkaConfig = KfUtils09.createKafkaConfig(kafkaBrokers, consumerGroup)
      KfUtils09.commitOffsets(offsetsToCommit, kafkaConfig)
      true
    }.getOrElse(false)
  }
}

/*

//testing code

import com.goibibo.dp.utils.{SparkKafkaUtils,ZkUtils,KfUtils}

implicit val zk = ZkUtils.connect("127.0.0.1:2181/apps/hotel_etl").get

val broker = "127.0.0.1"
val topics = Seq("test_hotel_etl", "test_flight_etl")
val consumerGroup = "testetl1"

val (rddKafka, offsetsToCommit) = SparkKafkaUtils.createRdd(broker, topics, consumerGroup, sc)
rddKafka.collect
KfUtils.commitOffsets(consumerGroup, offsetsToCommit)

*/



