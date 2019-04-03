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

/*
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import com.goibibo.dp.utils.SparkRedshiftUtil._


System.setProperty("fs.s3a.endpoint", "s3-ap-south-1.amazonaws.com");
System.setProperty("com.amazonaws.services.s3.enableV4", "true");
sc.hadoopConfiguration.set("fs.s3a.endpoint","s3-ap-south-1.amazonaws.com")

val row1 = Row(1, true, "a string", null)
val row2 = Row(2, true, "2 strings", null)

def metaLength(varcharSize: Long) = {
		new MetadataBuilder().putLong("maxlength", varcharSize).build()
}

def getSchema = StructType(Array(
		StructField("id", IntegerType, true),
		StructField("isEnabled", BooleanType, true),
		StructField("data", StringType, true, metaLength(128)),
		StructField("transactionid", StringType, true, metaLength(28))))

val rddData = sc.parallelize(Seq(row1,row2))
val df = sqlContext.createDataFrame(rddData, getSchema)
val redshiftUrl = "jdbc:redshift://redshift_host:5439/goibibo"
val tempS3Location = "s3a://.../redshift_upload/"
val writeMode = "overwrite"
val tableName = "testdp.test1"
val redshiftConf = RedshiftWriterSettings(redshiftUrl,tempS3Location,writeMode,tableName,None)
storeToRedshift(df, sc.getConf, redshiftConf)

val row1 = Row(1, false, "a string", null)
val row3 = Row(3, true, "3 strings", null)
val rddData = sc.parallelize(Seq(row1,row3))
val df = sqlContext.createDataFrame(rddData, getSchema)
val writeMode = "staging"
val redshiftConf = RedshiftWriterSettings(redshiftUrl, tempS3Location, writeMode, tableName, Some("id"))
storeToRedshift(df, sc.getConf, redshiftConf)
*/
