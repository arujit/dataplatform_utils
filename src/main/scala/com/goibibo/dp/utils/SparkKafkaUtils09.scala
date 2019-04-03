package com.goibibo.dp.utils

import org.apache.spark.streaming.kafka._
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import kafka.serializer.StringDecoder
import collection.JavaConverters._
import scala.util.Try
import org.slf4j.LoggerFactory
import kafka.message.MessageAndMetadata
import org.apache.spark.streaming.kafka.Broker
import scala.reflect.ClassTag
import kafka.serializer.Decoder
import kafka.common.TopicAndPartition

object SparkKafkaUtils09 {
	
	type KOffsets = Map[TopicPartition,Long]
	private val logger = LoggerFactory.getLogger(SparkKafkaUtils09.getClass)

	def createOffsetRange(kafkaBrokers:String, topics:Seq[String], 
		consumerGroup:String, maxMessagesPerPartition:Option[Int]) : (Seq[OffsetRange], KOffsets)  = {
		
		val kafkaConfig 	= KfUtils09.createKafkaConfig(kafkaBrokers,consumerGroup)
		val topicsNames 	= topics.asJava
		val latestOffsets 	= KfUtils09.getLatestOffsets(topicsNames, kafkaConfig).get
		val commitedOffsets = KfUtils09.getCommitedOffsets(topicsNames, kafkaConfig).get
		val offsetRanges	= latestOffsets.toList.map( (pairTopicPartitionAndOffset) => {
			val (tp, untilOffset) = pairTopicPartitionAndOffset
			val totalMessagesInPartition = untilOffset - commitedOffsets(tp)
			logger.info(s"${tp.topic} ${tp.partition} commitedOffset = ${commitedOffsets(tp)} untilOffset = ${untilOffset}")
			logger.info(s"${tp.topic} ${tp.partition} totalMessagesInPartition = ${totalMessagesInPartition} ")
			val newUntilOffset = if(maxMessagesPerPartition.isDefined) {
				if(totalMessagesInPartition > maxMessagesPerPartition.get) {
					logger.info(s"${tp.topic} ${tp.partition} totalMessagesInPartition = ${totalMessagesInPartition} higher than maxMessagesPerPartition = ${maxMessagesPerPartition}")
					val newUntilOffset = commitedOffsets(tp) + maxMessagesPerPartition.get
					logger.info(s"${tp.topic} ${tp.partition} new untilOffset = ${newUntilOffset}")
					newUntilOffset
				} else {
					untilOffset
				}
			} else {
				untilOffset
			}
			
			OffsetRange.create(tp.topic, tp.partition, commitedOffsets(tp), newUntilOffset)
		})
		val latestOffsetsN = offsetRanges.map( o => (new TopicPartition(o.topic,o.partition), o.untilOffset) ).toMap
		(offsetRanges, latestOffsetsN)
	}

	def createRdd(kafkaBrokers:String, topics:Seq[String], consumerGroup:String,
		sc:SparkContext, maxMessagesPerPartition:Option[Int] = None) : ( RDD[(String, String)], KOffsets ) = {
		val (offsets,latestOffsets) = createOffsetRange(kafkaBrokers, topics, consumerGroup, maxMessagesPerPartition)
		val kafkaParams = Map("bootstrap.servers" -> kafkaBrokers)
		val rddKafka = KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](
								sc, kafkaParams, offsets.toArray)
		(rddKafka, latestOffsets)
	}

	def createRddG[K:ClassTag,V:ClassTag,KD <: Decoder[K]: ClassTag,VD <: Decoder[V]: ClassTag](
		kafkaBrokers:String, topics:Seq[String], consumerGroup:String,
		sc:SparkContext, maxMessagesPerPartition:Option[Int] = None) : ( RDD[(K, V)], KOffsets ) = {
		val (offsets,latestOffsets) = createOffsetRange(kafkaBrokers, topics, consumerGroup, maxMessagesPerPartition)
		val kafkaParams = Map("bootstrap.servers" -> kafkaBrokers)
		val rddKafka = KafkaUtils.createRDD[K, V, KD, VD](
								sc, kafkaParams, offsets.toArray)
		(rddKafka, latestOffsets)
	}

	def createRddF[K:ClassTag,V:ClassTag,
		KD <: Decoder[K]: ClassTag,
		VD <: Decoder[V]: ClassTag,
		R:ClassTag](
		kafkaBrokers:String, topics:Seq[String], 
		consumerGroup:String,
		sc:SparkContext, 
		messageHandler: (MessageAndMetadata[K, V]) ⇒ R,
		maxMessagesPerPartition:Option[Int] = None) : ( RDD[R], KOffsets ) = {

		val (offsets,latestOffsets) = createOffsetRange(kafkaBrokers, topics, 
			consumerGroup, maxMessagesPerPartition)
		val kafkaParams = Map("bootstrap.servers" -> kafkaBrokers)
		val leadersMap = Map[TopicAndPartition,Broker]()
		val rddKafka = KafkaUtils.createRDD[K, V, KD, VD,R](
								sc, kafkaParams, offsets.toArray, leadersMap, 
								messageHandler)
		(rddKafka, latestOffsets)

	}

	def commitOffsets(kafkaBrokers:String, consumerGroup:String, offsetsToCommit:KOffsets):Boolean = {
		Try{
			val kafkaConfig = KfUtils09.createKafkaConfig(kafkaBrokers,consumerGroup)
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



