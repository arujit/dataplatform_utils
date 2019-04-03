package com.goibibo.dp.utils

import kafka.common.TopicAndPartition
import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka._
import org.apache.zookeeper.ZooKeeper

object SparkKafkaUtils {

    type KOffsets = Seq[(TopicAndPartition, Long)]

    def createOffsetRange(kafkaBroker: String, topics: Seq[String],
                          consumerGroup: String)(implicit zk: ZooKeeper): (Seq[OffsetRange], KOffsets) = {

        val consumer = KfUtils.createKafkaConsumer(kafkaBroker)
        val topicPartitions = KfUtils.getPartitionsForTopics(consumer, topics)
        val latestOffsets = KfUtils.getLatestOffsets(consumer, topicPartitions)
        val commitedOffsets = KfUtils.getCommitedOffsets(consumer, consumerGroup, topicPartitions).toMap
        val offsetRanges = latestOffsets.map((pairTopicPartitionAndOffset) => {
            val (tp, untilOffset) = pairTopicPartitionAndOffset
            OffsetRange.create(tp.topic, tp.partition, commitedOffsets(tp), untilOffset)
        })
        consumer.close
        (offsetRanges, latestOffsets)
    }

    def createRdd(kafkaBroker: String, topics: Seq[String], consumerGroup: String,
                  sc: SparkContext)(implicit zk: ZooKeeper): (RDD[(String, String)], KOffsets) = {
        val (offsets, latestOffsets) = SparkKafkaUtils.createOffsetRange(kafkaBroker, topics, consumerGroup)
        val kafkaParams = Map("bootstrap.servers" -> (kafkaBroker + ":9092"))
        val rddKafka = KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](sc, kafkaParams, offsets.toArray)
        (rddKafka, latestOffsets)
    }
}
