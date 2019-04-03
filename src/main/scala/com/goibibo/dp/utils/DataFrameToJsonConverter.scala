package org.apache.spark.sql

import java.io.CharArrayWriter

import com.fasterxml.jackson.core.JsonFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow

import scala.reflect.ClassTag

//Hack to access Spark's private variable 
object DataFrameToJsonConverter {
    def toJson[T: ClassTag](df: DataFrame, makeRow: (InternalRow, String) => T): RDD[T] = {
        val queryExecutionField = df.getClass.getDeclaredFields.filter(_.getName.contains("queryExecution"))(0)
        queryExecutionField.setAccessible(true)
        val queryExecution = queryExecutionField.get(df).asInstanceOf[execution.QueryExecution]

        def jacksonGenrator = execution.datasources.json.JacksonGenerator

        val rowSchema = df.schema
        queryExecution.toRdd.mapPartitions { iter =>
            val writer = new CharArrayWriter()
            // create the Generator without separator inserted between 2 records
            val gen = new JsonFactory().createGenerator(writer).setRootValueSeparator(null)

            new Iterator[T] {
                override def hasNext: Boolean = iter.hasNext

                override def next(): T = {
                    val row = iter.next()
                    jacksonGenrator.apply(rowSchema, gen)(row)
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
