package dlr.datatypes

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructField, StructType, DateType}

/**
  * Created by dyana on 10/04/16.
  */
object DateTest {
  def RunTest(sc: SparkContext, sqlContext: SQLContext) = {
    // The value will throw an error if it can't be parsed by java.sql.Date.valueOf ex. '0000-00-00'
    val schema = StructType(StructField("date", DateType, true)::Nil)
    val inputData = Seq(
      "{'date': '2016-04-24'}",
      "{'date': '0001-01-01'}",
      "{'date': '9999-12-31'}",
      "{'date': '2016-04-24 12:10:01'}",
      "{'date': 1461496201000}",
      "{'date': null}")

    val rdd = sc.parallelize(inputData)

    Printer.PrintDataExample(sqlContext, "DateType", rdd, schema, inputData.mkString("\n"))
  }
}
