package com.goibibo.dp.utils

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}
import resource._

import scala.util.Try

object JdbcReader {
    private val logger: Logger = LoggerFactory.getLogger(JdbcReader.getClass)

    case class DBConfiguration(database: String,
                               db: String,
                               schema: String,
                               tableName: String,
                               hostname: String,
                               portNo: Int,
                               userName: String,
                               password: String)

    /* 
        distKey: If set as None, then we try to find the dist key using the primary key of the table
                 If set as Some, We use the provide field to partition the table
                 Note: If there is more than one field in your primary key then please set distKey manually
                 Note: We avoid a query to database, If distKey is already provided.
        
        partitions: If set we divide the input dataframe in provided partitions 
                    Else we use below formula to decide the partitionsCount
                         partitionsCount = totalRecordsInTable / (executorMemory/avgRecordSizeOfTable)
        isDistKeyTimestamp: This variable is a hack, 
                            If you set this to true 
                                Ensure that you provide distKey and partitionTimeStampDuration 
    */
    case class PartitionSettings(distKey: Option[String] = None,
                                 partitions: Option[Int] = None,
                                 executorMemoryToDataRatio: Float = 0.8F,
                                 isDistKeyTimestamp: Boolean = false)

    type FromValue = String
    type ToValue = String
    type FieldName = String
    type OffsetValue = String
    type FieldNameWithRange = (FieldName, FromValue, ToValue)
    type FieldNamesWithRange = Seq[FieldNameWithRange]

    /*
        partitionSettings: If partitionSettings is set to none, We try to auto-partition the table
        incrementalUpdateFields: If incrementalUpdateFields is set to none, we load entire table in the RDD else 
            we get the filtered table based on FieldNamesWithRange
    */
    case class InternalConfig(incrementalUpdateFields: Option[FieldNamesWithRange],
                              partitionSettings: PartitionSettings = PartitionSettings(),
                              whereCondition: Option[String] = None,
                              selectColumns: Option[List[String]] = None)

    def getDataFrame(sqlContext: SQLContext, mysqlConfig: DBConfiguration, internalConfig: InternalConfig): DataFrame = {

        val combinedWhereCondition = getWhereConditionWithOffsets(internalConfig)
        logger.info(s"combinedWhereCondition = $combinedWhereCondition")

        val partitions: Seq[String] = getPartitions(sqlContext, mysqlConfig, internalConfig, combinedWhereCondition)
        logger.info(s"partitions = \n ${partitions.foreach(println)}")

        val properties = new Properties()
        properties.setProperty("user", mysqlConfig.userName)
        properties.setProperty("password", mysqlConfig.password)

        val df = sqlContext.read.
                option("driver", "com.mysql.jdbc.Driver").
                option("user", mysqlConfig.userName).
                option("password", mysqlConfig.password).
                jdbc(getJDBCUrl(mysqlConfig), mysqlConfig.tableName, partitions.toArray, properties)
        if (internalConfig.selectColumns.isDefined) {
            val columns: List[String] = internalConfig.selectColumns.get
            df.select(columns.head, columns.tail: _*)
        }
        else
            df
    }

    case class ConsumerGroupConfig(consumerGroup: String, zkUrl: String, incrementalUpdateFields: FieldNamesWithRange)

    def getDataFrameWithConsumerGroup(
                                             sqlContext: SQLContext,
                                             mysqlConfig: DBConfiguration,
                                             consumerGroupConfig: ConsumerGroupConfig,
                                             partitionSettings: PartitionSettings,
                                             whereCondition: Option[String]): (DataFrame, ConsumerGroupConfig) = {

        val commitedOffsets: Seq[(FieldName, OffsetValue)] = getCommitedOffsets(consumerGroupConfig)
        logger.info(s"Commited offsets are  \n = ${commitedOffsets.foreach(println)}")
        val offsets: FieldNamesWithRange = getOffsets(consumerGroupConfig.incrementalUpdateFields, commitedOffsets.toMap)
        logger.info(s"Combined offsets are  \n = ${offsets.foreach(println)}")
        val internalConfig = InternalConfig(Some(offsets), partitionSettings, whereCondition)
        val df = getDataFrame(sqlContext, mysqlConfig, internalConfig)
        val updatedConsumerGroup = consumerGroupConfig.copy(incrementalUpdateFields = offsets)
        (df, updatedConsumerGroup)
    }

    //case class PartitionSettings( distKey: Option[String] = None, partitions: Option[Int] = None)
    def getPartitions(sqlContext: SQLContext, mysqlConfig: DBConfiguration,
                      internalConfig: InternalConfig, combinedWhereCondition: String): Seq[String] = {
        val distKeyProvided = internalConfig.partitionSettings.distKey
        val partitionsProvided = internalConfig.partitionSettings.partitions
        val isDistKeyTimestamp = internalConfig.partitionSettings.isDistKeyTimestamp
        val executorMemoryToDataRatio = internalConfig.partitionSettings.executorMemoryToDataRatio
        val allRecordsCondition = "1 = 1"
        //If user wants only one partition then let us not do any processing
        if (partitionsProvided.isDefined && partitionsProvided.get == 1) {
            if (combinedWhereCondition != "") {
                logger.info(s"User has requested for the single partition, partition condition is $combinedWhereCondition")
                Seq(combinedWhereCondition)
            } else {
                logger.info(s"User has requested for a single partition, allRecordsCondition = $allRecordsCondition")
                Seq[String](allRecordsCondition) //Return all the records in single partition
            }
        } else {
            implicit val connection = getConnection(mysqlConfig)
            //If distkey is not provided, use the primary key
            val result = getDistKey(mysqlConfig, distKeyProvided) match {
                case Some(distKey) =>
                    logger.info(s"distKey found, it is ${distKey}")
                    getMinMaxForDistKey(mysqlConfig, distKey, combinedWhereCondition, isDistKeyTimestamp) match {
                        case Some(minMax) => {
                            logger.info(s"MinMax for the distKey=${distKey} is ${minMax}")
                            val recordsPerTask = getRecordsPerTask(sqlContext, mysqlConfig,
                                partitionsProvided, minMax,
                                executorMemoryToDataRatio)
                            val partitionsC = if (isDistKeyTimestamp) {
                                getPartitionsInternalT(mysqlConfig, minMax, recordsPerTask, distKey)
                            } else {
                                getPartitionsInternal(minMax, recordsPerTask, distKey)
                            }

                            val partitions = if (partitionsC.isEmpty) {
                                logger.warn(s"Received partitionsCount = 0, sending allRecordsCondition")
                                Seq[String](allRecordsCondition)
                            } else {
                                partitionsC
                            }

                            partitions.map(c => {
                                if (combinedWhereCondition.isEmpty) c
                                else c + s" AND ($combinedWhereCondition)"
                            })
                        }
                        case None =>
                            logger.warn("No min, max found in the table")
                            Seq[String]()
                    }
                case None =>
                    logger.warn("No primary key/more than one primary keys found in the table")
                    Seq[String]()
            }

            //If partitions are not provides then detect the number of partitions
            Try {
                connection.close()
            }
            result
        }
    }

    def getOffsets(incrementalUpdateFields: FieldNamesWithRange, commitedOffsets: Map[FieldName, OffsetValue]): FieldNamesWithRange = {
        incrementalUpdateFields.map { pair => {
            val fieldName = pair._1
            val defaultFromValue = pair._2
            val fromValue = commitedOffsets.getOrElse(fieldName, defaultFromValue)
            if (fromValue != defaultFromValue) {
                logger.info(s"Replaced default with commited for ${fieldName} default=${defaultFromValue} commited=${fromValue}")
            }
            val toValue = pair._3
            (fieldName, fromValue, toValue)
        }
        }
    }

    def getWhereConditionWithOffsets(internalConfig: InternalConfig): String = {
        val explicitWhere = internalConfig.whereCondition.map(_ + " and ").getOrElse("")
        val constructedWhere = internalConfig.incrementalUpdateFields.map {
            _.flatMap(tpl => {
                val (fieldName, fromValue, toValue) = tpl
                val fromCondition = if (fromValue != "") s"${fieldName} >= ${fromValue}" else ""
                val andCondition = if (fromCondition != "") " and " else ""
                val toCondition = if (toValue != "") s"${fieldName} < ${toValue}" else ""
                val jointCondition = s"${fromCondition}${andCondition}${toCondition}"
                if (jointCondition != "") Seq(s"(${jointCondition})") else Seq[String]()
            }).mkString(" or ")
        }.getOrElse("")
        logger.info(s"combinedWhereCondition = ${explicitWhere} ${constructedWhere}")
        explicitWhere + constructedWhere
    }

    def getMinMaxForDistKey(conf: DBConfiguration, distKey: String, whereCondition: String,
                            isDistKeyTimestamp: Boolean)(
                                   implicit con: Connection): Option[(Long, Long)] = {

        var query = if (!isDistKeyTimestamp) {
            s"SELECT min($distKey), max($distKey) FROM ${conf.db}.${conf.tableName}"
        } else {
            s"SELECT UNIX_TIMESTAMP(min($distKey)), UNIX_TIMESTAMP(max($distKey)) FROM ${conf.db}.${conf.tableName}"
        }

        if (!whereCondition.isEmpty()) {
            query += " WHERE " + whereCondition
        }

        logger.info("Running Query to get min,max: \n{}", query)

        Try {
            var minMax: Option[(Long, Long)] = None
            for (
                stmt <- managed(con.createStatement);
                rs <- managed(stmt.executeQuery(query))) {

                rs.next()
                val min: Long = rs.getLong(1)
                val max: Long = rs.getLong(2)
                minMax = Some((min, max))
                logger.info(s"Minimum $distKey: $min :: Maximum $distKey: $max")
            }
            minMax
        }.getOrElse(None)
    }

    def getRecordsPerTask(sqlContext: SQLContext, dbConf: DBConfiguration,
                          partitionsProvided: Option[Int], minMaxAndRows: (Long, Long),
                          executorMemoryToDataRatio: Float)
                         (implicit con: Connection): Int = {
        val minMaxDiff: Long = minMaxAndRows._2 - minMaxAndRows._1 + 1
        partitionsProvided match {
            case Some(partitions) => {
                Math.ceil(minMaxDiff / partitions).toInt
            }
            case None => {
                val memory: Long = getExecutorMemory(sqlContext.sparkContext.getConf)
                logger.info("Calculating number of partitions with each executor has memory: {}", memory)
                val avgRowSize: Long = getAvgRowSize(dbConf)
                if (avgRowSize == 0) {
                    logger.warn("Avg record size is 0")
                    0
                } else {
                    logger.info("Average Row size: {}, difference b/w min-max primary key: {}", avgRowSize, minMaxDiff)
                    val perTaskRecords = (memory / avgRowSize).toDouble * executorMemoryToDataRatio
                    logger.info(s"perTaskRows memory=${memory}/avgRowSize=${avgRowSize} * executorMemoryToDataRatio=${executorMemoryToDataRatio} => ${perTaskRecords}")
                    perTaskRecords.toInt
                }
            }
        }
    }

    def getPartitionsInternalT(conf: DBConfiguration, minMax: (Long, Long),
                               recordsPerTask: Int, distkey: String)
                              (implicit con: Connection): Seq[String] = {
        val query = s"SELECT ${distkey} FROM ${conf.db}.${conf.tableName} WHERE ${distkey} >= FROM_UNIXTIME(${minMax._1}) AND ${distkey} <= FROM_UNIXTIME(${minMax._2})"
        var partitions = Seq[String]()
        for (
            stmt <- managed(con.createStatement);
            rs <- managed(stmt.executeQuery(query))) {

            var partitionPoints = IndexedSeq[Long](minMax._1)
            var counter: Int = 0
            while (rs.next) {
                counter = counter + 1
                if (counter % recordsPerTask == 0) {
                    val distkeyValue = rs.getTimestamp(1).getTime / 1000
                    partitionPoints = partitionPoints :+ distkeyValue
                }
            }
            partitionPoints = partitionPoints :+ (minMax._2 + 1)
            partitions = partitionPoints.zipWithIndex.view(0, partitionPoints.size - 1).map {
                case (strP, idx) => (strP, partitionPoints(idx + 1))
            }.map { case (fromT, toT) =>
                s"""(   ${distkey} >= FROM_UNIXTIME(${fromT}) AND ${distkey} < FROM_UNIXTIME(${toT}) )"""
            }
        }
        partitions
    }

    def getPartitionsInternal(minMax: (Long, Long), recordsPerTask: Int, distKey: String)(implicit con: Connection): Seq[String] = {
        val minMaxDiff: Long = minMax._2 - minMax._1 + 1
        var partitions: Int = Math.ceil(minMaxDiff / recordsPerTask).toInt
        logger.info("Total number of partitions are {}", partitions)

        if (partitions == 0) {
            logger.warn(s"Received partitionsCount = 0, sending allRecordsCondition")
            Seq[String]()
        } else {
            val minV = minMax._1
            val maxV = minMax._2
            val nr: Long = maxV - minV
            val inc: Long = Math.ceil(nr.toDouble / partitions).toLong
            (0 until partitions).toList.map { n =>
                val fromValue = minV + n * inc
                val toValue = {
                    if (n < partitions - 1) (minV + (n + 1) * inc) - 1
                    else maxV
                }
                s"$distKey BETWEEN ${fromValue} AND ${toValue}"
            }
        }
    }

    def getExecutorMemory(conf: SparkConf): Long = {
        val KB: Long = 1024
        val MB: Long = KB * KB
        val GB: Long = KB * MB

        val defaultExecutorMemorySize = 512 * MB
        val executorMemorySize = Try {
            conf.getSizeAsBytes("spark.executor.memory")
        }.getOrElse {
            logger.warn("Wrong format of executor memory, Taking default {}", defaultExecutorMemorySize)
            defaultExecutorMemorySize
        }
        logger.info("executorMemorySize = {}", executorMemorySize)
        executorMemorySize
    }

    def getDistKey(conf: DBConfiguration, distKeyProvided: Option[String])
                  (implicit con: Connection): Option[String] = {

        distKeyProvided match {
            case Some(key) => {
                logger.info("Found distKey in configuration {}", key)
                Some(key)
            }
            case None => {
                logger.info("Found no distKey in configuration")
                val meta = con.getMetaData
                val resPrimaryKeys = meta.getPrimaryKeys(conf.db, null, conf.tableName)
                var primaryKeys = scala.collection.immutable.Set[String]()

                while (resPrimaryKeys.next) {
                    val columnName = resPrimaryKeys.getString(4)
                    primaryKeys = primaryKeys + columnName
                }

                Try {
                    resPrimaryKeys.close()
                }
                if (primaryKeys.size != 1) {
                    logger.error(s"Found multiple primary keys, Not taking any. ${primaryKeys.mkString(",")}")
                    None
                } else {
                    logger.info(s"Found primary key, using it as distribution key, ${primaryKeys.toSeq.head}")
                    Some(primaryKeys.toSeq.head)
                }
            }
        }
    }

    def commitOffsets(cgConfig: ConsumerGroupConfig): Unit = {

        implicit val zk = ZkUtils.connect(cgConfig.zkUrl).get
        val offsetsLocation = "/consumers/" + cgConfig.consumerGroup + "/offsets"
        ZkUtils.deleteNodeR(offsetsLocation)
        KfUtils.createConsumerGroupNode(cgConfig.consumerGroup, true)
        val topics = cgConfig.incrementalUpdateFields.foreach(tuple => {
            val fieldName = tuple._1
            val offset = tuple._3
            val nodePath = offsetsLocation + "/" + fieldName
            ZkUtils.createPersistentNode(nodePath, offset)
        })
        ZkUtils.close
    }

    def getCommitedOffsets(cgConfig: ConsumerGroupConfig): Seq[(FieldName, OffsetValue)] = {
        implicit val zk = ZkUtils.connect(cgConfig.zkUrl).get
        val results = if (cgConfig.consumerGroup == "") {
            Seq[(FieldName, OffsetValue)]()
        } else {
            val offsetsNode = KfUtils.createConsumerGroupNode(cgConfig.consumerGroup, true)
            val listFields = ZkUtils.getChildren(offsetsNode)
            listFields.get.map { (field) =>
                val fieldNode = offsetsNode + s"/${field}"
                val offsetValue = ZkUtils.getNodeDataAsString(fieldNode).get
                (field, offsetValue)
            }
        }
        ZkUtils.close
        results
    }

    def getConnection(conf: DBConfiguration): Connection = {
        val connectionProps = new Properties()
        connectionProps.put("user", conf.userName)
        connectionProps.put("password", conf.password)
        val connectionString = getJDBCUrl(conf)
        Class.forName("com.mysql.jdbc.Driver")
        DriverManager.getConnection(connectionString, connectionProps)
    }

    def getJDBCUrl(conf: DBConfiguration): String = {
        val jdbcUrl = s"jdbc:${conf.database}://${conf.hostname}:${conf.portNo}/${conf.db}"
        if (conf.database.toLowerCase == "mysql")
            jdbcUrl + "?zeroDateTimeBehavior=convertToNull"
        else jdbcUrl
    }

    def getAvgRowSize(mysqlDBConf: DBConfiguration)(implicit con: Connection): Long = {
        logger.info(s"Calculating average row size: ${mysqlDBConf.db}.${mysqlDBConf.tableName}")
        val query = s"SELECT avg_row_length FROM information_schema.tables WHERE table_schema = " +
                s"'${mysqlDBConf.db}' AND table_name = '${mysqlDBConf.tableName}'"
        val result: ResultSet = con.createStatement().executeQuery(query)
        val avgSize: Long = Try {
            result.next;
            result.getLong(1)
        }.getOrElse(0)
        Try {
            result.close
        }
        avgSize
    }

    val defaultOffsetValue = ""
}

