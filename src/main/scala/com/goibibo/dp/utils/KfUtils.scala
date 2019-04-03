package com.goibibo.dp.utils

import kafka.api.PartitionOffsetRequestInfo
import kafka.common.TopicAndPartition
import kafka.javaapi._
import kafka.javaapi.consumer.SimpleConsumer
import org.apache.zookeeper.ZooKeeper

import scala.collection.JavaConverters._

object KfUtils {
    type KOffsets = Seq[(TopicAndPartition, Long)]

    def createKafkaConsumer(broker: String, port: Int = 9092, clientId: String = "client"): SimpleConsumer = {
        val socketTimeout = 100000
        val socketBufferSize = 64 * 1024
        new SimpleConsumer(broker, port, socketTimeout, socketBufferSize, clientId)
    }

    def getPartitionsForTopics(consumer: SimpleConsumer, topics: Seq[String]): Seq[TopicAndPartition] = {
        val req = new TopicMetadataRequest(topics.asJava)
        val resp: TopicMetadataResponse = consumer.send(req)
        resp.topicsMetadata.asScala.flatMap { (topicMetadata) => {
            val topic = topicMetadata.topic
            topicMetadata.partitionsMetadata.asScala.map { (partitionsMetadata) => {
                val partitionId = partitionsMetadata.partitionId
                TopicAndPartition(topic, partitionId)
            }
            }
        }
        }
    }

    def getOffsets(time: Long)(consumer: SimpleConsumer, topicsPartitions: Seq[TopicAndPartition]): KOffsets = {
        val requestInfo: Map[TopicAndPartition, PartitionOffsetRequestInfo] = topicsPartitions.map {
            (_, PartitionOffsetRequestInfo(time, 1))
        }.toMap
        val req = new OffsetRequest(requestInfo.asJava, kafka.api.OffsetRequest.CurrentVersion, "clientid")
        val resp: OffsetResponse = consumer.getOffsetsBefore(req)
        topicsPartitions.map { (topicPartition) => {
            val offset = resp.offsets(topicPartition.topic, topicPartition.partition)(0)
            (topicPartition, offset)
        }
        }
    }

    def getLatestOffsets: (SimpleConsumer, Seq[TopicAndPartition]) => KOffsets = getOffsets(kafka.api.OffsetRequest.LatestTime) _

    def getEarliestOffsets: (SimpleConsumer, Seq[TopicAndPartition]) => KOffsets = getOffsets(kafka.api.OffsetRequest.EarliestTime) _

    def getCommitedOffsets(consumer: SimpleConsumer, consumerGroup: String,
                           topicsPartitions: Seq[TopicAndPartition])(implicit zk: ZooKeeper): KOffsets = {
        (
                getEarliestOffsets(consumer, topicsPartitions).toMap ++
                        getCommitedOffsetsInternal(consumerGroup, topicsPartitions).toMap
                ).toSeq
    }

    def commitOffsets(consumerGroup: String,
                      topicsPartitions: KOffsets)(implicit zk: ZooKeeper): Unit = {

        //TODO: Make strings consts
        val offsetsLocation = "/consumers/" + consumerGroup + "/offsets"
        ZkUtils.deleteNodeR(offsetsLocation)
        createConsumerGroupNode(consumerGroup, failSilently = true)
        val topics = topicsPartitions.map(_._1.topic).toSet
        topics.foreach { (topic) =>
            ZkUtils.createPersistentNode(offsetsLocation + "/" + topic,
                s"topic node for $consumerGroup")
        }

        topicsPartitions.foreach((pair) => {
            val (tp, offset) = pair
            val nodePath = Seq(offsetsLocation, tp.topic, tp.partition).mkString("/")
            ZkUtils.createPersistentNode(nodePath, offset.toString)
        })
    }

    def createConsumerGroupNode(consumerGroup: String, failSilently: Boolean = false)(implicit zk: ZooKeeper): String = {
        val consumersParentNode = "/consumers"
        val consumerNode = consumersParentNode + "/" + consumerGroup
        val consumerOffsetsNode = consumerNode + "/offsets"
        ZkUtils.createPersistentNode(consumersParentNode, "parent node for etl consumers", failSilently)
        ZkUtils.createPersistentNode(consumerNode, s"etl consumer $consumerGroup", failSilently)
        ZkUtils.createPersistentNode(consumerOffsetsNode, s"offsets node for etl consumer $consumerGroup", failSilently)
        consumerOffsetsNode
    }

    def getCommitedOffsetsInternal(consumerGroup: String, topicsPartitions: Seq[TopicAndPartition]
                                  )(implicit zk: ZooKeeper): KOffsets = {

        val offsetsNode = createConsumerGroupNode(consumerGroup, failSilently = true)
        val listTopics = ZkUtils.getChildren(offsetsNode)
        listTopics.get.flatMap { (topic) =>
            val topicNode = offsetsNode + s"/$topic"
            ZkUtils.getChildren(topicNode).get.map { (partitionNode) =>
                val partitionId = partitionNode.toInt
                val offset = ZkUtils.getNodeDataAsString(topicNode + "/" + partitionNode).get.toLong
                (TopicAndPartition(topic, partitionId), offset)
            }
        }
    }

}


/*

//testing code
import com.goibibo.dp.utils.{KfUtils,ZkUtils}
implicit val zk = ZkUtils.connect("127.0.0.1:2181/apps/hotel_etl").get

val broker = "127.0.0.1"
val topics = Seq("test_hotel_etl", "test_flight_etl")
val consumerGroup = "testetl1"

val consumer 		= KfUtils.createKafkaConsumer(broker)
val topicPartitions = KfUtils.getPartitionsForTopics(consumer, topics)
val latestOffsets 	= KfUtils.getLatestOffsets(consumer, topicPartitions)
val earliestOffsets = KfUtils.getEarliestOffsets(consumer, topicPartitions)
val commitedOffsets = KfUtils.getCommitedOffsets(consumer, consumerGroup, topicPartitions)

}
*/



