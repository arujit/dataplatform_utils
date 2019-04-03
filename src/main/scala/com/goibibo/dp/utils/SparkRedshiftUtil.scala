package com.goibibo.dp.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SparkRedshiftUtil {

    case class RedshiftWriterSettings(redshiftUrl: String, tempS3Location: String, writeMode: String,
                                      tableName: String, mergeKey: Option[String] = None)

    def storeToRedshift(df: DataFrame, conf: SparkConf, writerConf: RedshiftWriterSettings,
                        customInsertStatement: Option[String] = None): Unit = {
        var tableName = writerConf.tableName
        val preActions = "SELECT 1;"
        var postActions = "SELECT 2;"
        var redshiftWriteMode = writerConf.writeMode
        var extracopyoptions = "TRUNCATECOLUMNS"

        if (redshiftWriteMode == "staging") {
            extracopyoptions += " COMPUPDATE OFF STATUPDATE OFF"
            redshiftWriteMode = "overwrite"
            val columns = df.schema.fields.map(_.name).mkString(",")
            val otable = writerConf.tableName
            val stagingTableName = writerConf.tableName + "_staging"
            tableName = stagingTableName
            val mergeKey = writerConf.mergeKey.get
            val insertStatement = if (customInsertStatement.isEmpty) {
                s"""INSERT INTO $otable ($columns)
                   			   |SELECT $columns FROM $stagingTableName;"""
            } else {
                customInsertStatement.get
            }
            postActions =
                    s"""
                       |DELETE FROM $otable USING $stagingTableName
                       |WHERE $otable.$mergeKey = $stagingTableName.$mergeKey;
                       |$insertStatement
                       |DROP TABLE $stagingTableName;""".stripMargin
        }

        df.write.
                format("com.databricks.spark.redshift").
                option("url", writerConf.redshiftUrl).
                option("user", conf.get("spark.redshift.username")).
                option("password", conf.get("spark.redshift.password")).
                option("jdbcdriver", "com.amazon.redshift.jdbc4.Driver").
                option("dbtable", tableName).
                option("tempdir", writerConf.tempS3Location).
                option("preactions", preActions).
                option("postactions", postActions).
                option("extracopyoptions", extracopyoptions).
                mode(redshiftWriteMode).
                save()
    }
}
