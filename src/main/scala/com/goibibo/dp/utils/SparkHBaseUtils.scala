package com.goibibo.dp.utils

import java.util

import org.apache.hadoop.hbase.client.{Connection, Get, Result, Table}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import scala.reflect.ClassTag


object SparkHBaseUtils {

	def bulkGetWithRow[T, U:ClassTag](hBaseContext:HBaseContext,
		tableName: TableName,
		batchSize: Integer,
		rdd: RDD[T],
		makeGet: (T) => Option[Get],
		convertResult: (T, Option[Result]) => U): RDD[U] = {

		val getMapPartition = new GetMapRowPartition(tableName, batchSize, makeGet, convertResult)

		hBaseContext.mapPartitions[T, U](rdd, getMapPartition.run)

	}

	private def transformResultsForGets[T,U](
			gets:java.util.ArrayList[Get],
			rows:java.util.ArrayList[T],
			convertResult: (T,Option[Result]) => U,
			table:Table): Seq[U] =  {
		val results = table.get(gets)
		val transformedResults = results.zipWithIndex.map{ pair =>
			val resultOfGet = pair._1
			val index = pair._2
			val row = rows(index)
			convertResult(row, Some(resultOfGet))
		}
		gets.clear()
		rows.clear()
		transformedResults
	}

	private class GetMapRowPartition[T, U](tableName: TableName,
		batchSize: Integer,
		makeGet: (T) => Option[Get],
		convertResult: (T,Option[Result]) => U)
	extends Serializable {

		val tName: Array[Byte] = tableName.getName

		def run(iterator: Iterator[T], connection: Connection): Iterator[U] = {
			val table: Table = connection.getTable(TableName.valueOf(tName))

			val gets: util.ArrayList[Get] = new java.util.ArrayList[Get]()
			val rows: util.ArrayList[T] = new java.util.ArrayList[T]()
			var res = List[U]()

			while (iterator.hasNext) {
				val itRow = iterator.next()
				rows.add(itRow)
				makeGet(itRow) match {
					case Some(getRequest) => gets.add(getRequest)
					case None => res = res :+ convertResult(itRow, None)
				}

				if (gets.size() == batchSize) {
					res = res ++ transformResultsForGets(gets, rows, convertResult, table)
				}
			}

			if (gets.size() > 0) {
				res = res ++ transformResultsForGets(gets, rows, convertResult, table)
			}

			table.close()
			res.iterator
		}
	}

	def getHbaseConf(zookeeperQuorum:String, zookeeperClientPort:String) : org.apache.hadoop.conf.Configuration = {
		val conf = HBaseConfiguration.create()
		//conf.set("hbase.zookeeper.quorum","hbasemaster01.prod.goibibo.com")
		//conf.set("hbase.zookeeper.property.clientPort","2181")
		conf.set("hbase.zookeeper.quorum", zookeeperQuorum)
		conf.set("hbase.zookeeper.property.clientPort", zookeeperClientPort)
		conf
	}


}

