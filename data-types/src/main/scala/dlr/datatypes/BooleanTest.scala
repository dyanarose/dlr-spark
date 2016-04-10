package dlr.datatypes

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}

/**
  * Created by dyana on 10/04/16.
  */
object BooleanTest {
  def RunTest(sc: SparkContext, sqlContext: SQLContext) = {
    val schema = StructType(StructField("boolean", BooleanType, true)::Nil)
    val inputData = Seq(
      "{'boolean': true}",
      "{'boolean': false}",
      "{'boolean': 'false'}",
      "{'boolean': 'true'}",
      "{'boolean': null}",
      "{'boolean': 1}",
      "{'boolean': 0}",
      "{'boolean': '1'}",
      "{'boolean': '0'}",
      "{'boolean': 'a'}"
    )
    val rdd = sc.parallelize(inputData)

    Printer.PrintDataExample(sqlContext, "BooleanType", rdd, schema, inputData.mkString("\n"))
  }
}
