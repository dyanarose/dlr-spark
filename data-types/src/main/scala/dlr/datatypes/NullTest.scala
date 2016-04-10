package dlr.datatypes

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{NullType, StructField, StructType}

/**
  * Created by dyana on 10/04/16.
  */
object NullTest {
  def RunTest(sc: SparkContext, sqlContext: SQLContext) = {
    val schema = StructType(StructField("null", NullType, true)::Nil)
    val inputData = Seq(
      "{'null': null}",
      "{'null': true}",
      "{'null': false}",
      "{'null': 1}",
      "{'null': 0}",
      "{'null': '1'}",
      "{'null': '0'}",
      "{'null': 'a'}"
    )
    val rdd = sc.parallelize(inputData)

    Printer.PrintDataExample(sqlContext, "NullType", rdd, schema, inputData.mkString("\n"))
  }
}
