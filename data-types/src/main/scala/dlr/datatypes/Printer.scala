package dlr.datatypes

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.{StructType}

/**
  * Created by dyana on 10/04/16.
  */
object Printer {
  def PrintDataExample(sqlContext:SQLContext, datatype: String, rdd: RDD[String], schema: StructType, example: String) = {
    val noSchemaDf = sqlContext.read.json(rdd)
    val schemaDf = sqlContext.read.schema(schema).json(rdd)

    println(s"-------------- $datatype --------------")
    println(s"------- $datatype Input")
    println(example)

    println
    println(s"------- $datatype Inferred Schema")
    Printer Print noSchemaDf

    println
    println(s"------- $datatype Set Schema")
    Printer Print schemaDf

    println
    println
  }
  def Print(df: DataFrame) = {
    df.printSchema()
    df.show
  }
}
