package dlr.datatypes

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{ShortType, StructField, StructType}

/**
  * Created by dyana on 10/04/16.
  */
object ShortTest {
  def RunTest(sc: SparkContext, sqlContext: SQLContext) = {
    val schema = StructType(StructField("short", ShortType, true)::Nil)
    val max = Short.MaxValue
    val min = Short.MinValue
    val int = Short.short2int(Short.MaxValue) + 1
    val inputData = Seq(
      "{'short': 0}",
      "{'short': 1}",
      s"{'short': $max}",
      s"{'short': $min}",
      s"{'short': $int}",
      "{'short': 1.23456}",
      "{'short': '0'}",
      "{'short': '1'}",
      "{'short': '1.23456'}",
      "{'short': null}")

    val rdd = sc.parallelize(inputData)

    Printer.PrintDataExample(sqlContext, "ShortType", rdd, schema, inputData.mkString("\n"))
  }
}
