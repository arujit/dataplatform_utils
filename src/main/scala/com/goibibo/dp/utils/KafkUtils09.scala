package com.goibibo.dp.utils

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._
import scala.util.Try

object KfUtils09 {

    def createKafkaConfig(brokers: String, consumerGroup: String, offsetReset: String = "earliest"): Properties = {
        val properties: Properties = new Properties()
        properties.setProperty("bootstrap.servers", brokers)
        properties.setProperty("group.id", consumerGroup)
        properties.setProperty("enable.auto.commit", "false")
        properties.setProperty("auto.offset.reset", offsetReset)
        properties.setProperty("value.deserializer", classOf[StringDeserializer].getName)
        properties.put("key.deserializer", classOf[StringDeserializer].getName)
        properties.put("value.deserializer", classOf[StringDeserializer].getName)
        properties
    }

    def getEarliestOffsetsI(kafkaConsumer: KafkaConsumer[String, String],
                            topicPartitions: util.Collection[TopicPartition]):
    Map[TopicPartition, Long] = {
        topicPartitions.asScala.foreach(kafkaConsumer.seekToBeginning(_))
        topicPartitions.asScala.map { p => (p, kafkaConsumer.position(p)) }.toMap
    }

    def getLatestOffsetsI(kafkaConsumer: KafkaConsumer[String, String],
                          topicPartitions: util.Collection[TopicPartition]
                         ): Map[TopicPartition, Long] = {
        topicPartitions.asScala.foreach(kafkaConsumer.seekToEnd(_))
        topicPartitions.asScala.map { p => (p, kafkaConsumer.position(p)) }.toMap
    }

    def getCommitedOffsetsI(kafkaConsumer: KafkaConsumer[String, String],
                            topicPartitions: util.Collection[TopicPartition]
                           ): Map[TopicPartition, Long] = {
        topicPartitions.asScala.map { (topicPartition) =>
            val position = kafkaConsumer.position(topicPartition)
            println(s"$topicPartition offset = $position")
            (topicPartition, position)
            // } else {
            // 	println(s"${topicPartition} offset Not found")
            // 	kafkaConsumer.seekToBeginning( topicPartition )
            // 	(topicPartition, kafkaConsumer.position(topicPartition) )
            // }
        }.toMap
    }

    def getOffset(topicNames: util.Collection[String], kafkaConfig: Properties,
                  offsetFetcher: (KafkaConsumer[String, String], util.Collection[TopicPartition]) => Map[TopicPartition, Long]):
    Option[Map[TopicPartition, Long]] = {
        try {
            val kafkaConsumer = new KafkaConsumer[String, String](kafkaConfig)
            val partitionInfo = topicNames.asScala.flatMap {
                kafkaConsumer.partitionsFor(_).asScala
            }
            val topicPartitions = partitionInfo.map(p => new TopicPartition(p.topic, p.partition))
            kafkaConsumer.assign(topicPartitions.toList.asJava)
            val result = Some(offsetFetcher(kafkaConsumer, topicPartitions.toList.asJava))
            Try {
                kafkaConsumer.close()
            }
            result
        } catch {
            case e: Exception => None
        }
    }

    def getEarliestOffsets(topicNames: util.Collection[String], kafkaConfig: Properties): Option[Map[TopicPartition, Long]] = {
        getOffset(topicNames, kafkaConfig, getEarliestOffsetsI)
    }

    def getLatestOffsets(topicNames: util.Collection[String], kafkaConfig: Properties): Option[Map[TopicPartition, Long]] = {
        getOffset(topicNames, kafkaConfig, getLatestOffsetsI)
    }

    def getCommitedOffsets(topicNames: util.Collection[String], kafkaConfig: Properties): Option[Map[TopicPartition, Long]] = {
        getOffset(topicNames, kafkaConfig, getCommitedOffsetsI)
    }

    def commitOffsets(offsets: Map[TopicPartition, Long], kafkaConfig: Properties): Boolean = {
        Try {
            val kafkaConsumer = new KafkaConsumer[String, String](kafkaConfig)
            val topics = offsets.toList.map(_._1.topic).toSet.toList.asJava
            val topicPartitions = offsets.toList.map(_._1).toList
            kafkaConsumer.assign(topicPartitions.asJava)
            val offsetsToCommit = offsets.toList.map { e =>
                val offset = new OffsetAndMetadata(e._2)
                (e._1, offset)
            }.toMap.asJava
            kafkaConsumer.commitSync(offsetsToCommit)
            Try {
                kafkaConsumer.close()
            }
            true
        }.getOrElse(false)
    }
}

//val prop = createKafkaConfig("localhost:9092","t1")
//val topicNames = Seq("test").asJava
//getCommitedOffsets(topicNames, prop)
//commitOffsets(toO.get,prop)