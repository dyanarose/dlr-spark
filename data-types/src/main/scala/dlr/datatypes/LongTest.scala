package dlr.datatypes

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{LongType, StructField, StructType}

/**
  * Created by dyana on 10/04/16.
  */
object LongTest {
  def RunTest(sc: SparkContext, sqlContext: SQLContext) = {
    val schema = StructType(StructField("long", LongType, true)::Nil)
    val max = Long.MaxValue
    val min = Long.MinValue
    val inputData = Seq(
      "{'long': 1}",
      s"{'long': $max}",
      s"{'long': $min}",
      "{'long': '1'}",
      "{'long': 1.23456}",
      "{'long': '1.23456'}",
      "{'long': null}")

    val rdd = sc.parallelize(inputData)

    Printer.PrintDataExample(sqlContext, "LongType", rdd, schema, inputData.mkString("\n"))
  }
}
