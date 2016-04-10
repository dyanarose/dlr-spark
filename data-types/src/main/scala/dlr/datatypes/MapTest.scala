package dlr.datatypes

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType}

/**
  * Created by dyana on 10/04/16.
  */
object MapTest {
  def RunTest(sc: SparkContext, sqlContext: SQLContext) = {
    val schema = StructType(StructField("map", MapType(StringType, StringType, true), true)::Nil)
    val inputData = Seq(
      "{'map': {'a_key': 'a value', 'b_key': 'b value'}}",
      "{'map': {'key': 1, 'key1': null}}",
      "{'map': null}")

    val rdd = sc.parallelize(inputData)

    Printer.PrintDataExample(sqlContext, "MapType", rdd, schema, inputData.mkString("\n"))
  }
}
