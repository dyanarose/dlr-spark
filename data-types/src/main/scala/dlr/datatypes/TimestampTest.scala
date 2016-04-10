package dlr.datatypes

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{TimestampType, StructField, StructType}

/**
  * Created by dyana on 10/04/16.
  */
object TimestampTest {
  def RunTest(sc: SparkContext, sqlContext: SQLContext) = {
    // The value will throw an error if it can't be parsed by java.sql.Timestamp.valueOf ex. '0000-00-00'
    val schema = StructType(StructField("timestamp", TimestampType, true)::Nil)
    val inputData = Seq(
      "{'timestamp': '2016-04-24'}",
      "{'timestamp': '2016-04-24 12:10:01'}",
      "{'timestamp': 1461496201000}",
      "{'timestamp': '0001-01-01'}",
      "{'timestamp': '9999-12-31'}",
      "{'timestamp': null}")

    val rdd = sc.parallelize(inputData)

    Printer.PrintDataExample(sqlContext, "TimestampType", rdd, schema, inputData.mkString("\n"))
  }
}
