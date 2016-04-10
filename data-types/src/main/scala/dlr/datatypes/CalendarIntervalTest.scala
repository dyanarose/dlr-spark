package dlr.datatypes

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{CalendarIntervalType, StructField, StructType}

/**
  * Created by dyana on 10/04/16.
  */
object CalendarIntervalTest {
  def RunTest(sc: SparkContext, sqlContext: SQLContext) = {
    // See:
    // https://github.com/apache/spark/blob/master/common/unsafe/src/main/java/org/apache/spark/unsafe/types/CalendarInterval.java
    // for how to specify a Calendar Interval
    val schema = StructType(StructField("calendarInterval", CalendarIntervalType, true)::Nil)
    val inputData = Seq(
      "{'calendarInterval': 'interval 2 days'}",
      "{'calendarInterval': 'interval 1 week'}",
      "{'calendarInterval': 'interval 5 years'}",
      "{'calendarInterval': 'interval 6 months'}",
      "{'calendarInterval': 10}",
      "{'calendarInterval': 'interval a'}",
      "{'calendarInterval': null}")

    val rdd = sc.parallelize(inputData)

    Printer.PrintDataExample(sqlContext, "CalendarIntervalType", rdd, schema, inputData.mkString("\n"))
  }
}
