package org.apache.spark.sql

import java.io.CharArrayWriter
import com.fasterxml.jackson.core.JsonFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import collection.JavaConverters._
import scala.reflect.ClassTag

//Hack to access Spark's private classes in org.apache.spark.sql schema
object DataFrameToJsonConverter {
    def toJson[T: ClassTag](df: DataFrame, makeRow: (InternalRow, String) => T,
                            timezone:String = "GMT+05:30",
                            timestampFormat:String = "yyyy-MM-dd hh:mm:ss.S"): RDD[T] = {
        val queryExecutionField = df.getClass.getDeclaredFields.filter(_.getName.contains("queryExecution"))(0)
        queryExecutionField.setAccessible(true)
        val queryExecution = queryExecutionField.get(df).asInstanceOf[execution.QueryExecution]

        val rowSchema = df.schema
        queryExecution.toRdd.mapPartitions { iter =>
            val writer = new CharArrayWriter()
            // create the Generator without separator inserted between 2 records
            val gen = new JsonFactory().createGenerator(writer).setRootValueSeparator(null)
            val options = new catalyst.json.JSONOptions(
                Map("timestampFormat" -> timestampFormat), timezone)
            val jacksonGenrator = new catalyst.json.JacksonGenerator(
                rowSchema, writer, options)
            new Iterator[T] {
                override def hasNext: Boolean = iter.hasNext

                override def next(): T = {
                    val row = iter.next()
                    jacksonGenrator.write(row)
                    gen.flush()
                    val json = writer.toString
                    if (hasNext) {
                        writer.reset()
                    } else {
                        gen.close()
                    }
                    makeRow(row, json)
                }
            }
        }
    }
}