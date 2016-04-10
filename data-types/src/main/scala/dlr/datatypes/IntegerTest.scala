package dlr.datatypes

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
  * Created by dyana on 10/04/16.
  */
object IntegerTest {
  def RunTest(sc: SparkContext, sqlContext: SQLContext) = {
    val schema = StructType(StructField("integer", IntegerType, true)::Nil)
    val long: Long = Int.int2long(Int.MaxValue) + 1L
    val max = Int.MaxValue
    val min = Int.MinValue
    val inputData = Seq(
      "{'integer': 1}",
      s"{'integer': $max}",
      s"{'integer': $min}",
      s"{'integer': $long}",
      "{'integer': '1'}",
      "{'integer': 1.23456}",
      "{'integer': '1.23456'}",
      "{'integer': null}")

    val rdd = sc.parallelize(inputData)

    Printer.PrintDataExample(sqlContext, "IntegerType", rdd, schema, inputData.mkString("\n"))
  }
}
