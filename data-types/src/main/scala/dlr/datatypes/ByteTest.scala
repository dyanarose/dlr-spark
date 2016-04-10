package dlr.datatypes

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{ByteType, StructField, StructType}

/**
  * Created by dyana on 10/04/16.
  */
object ByteTest {
  def RunTest(sc: SparkContext, sqlContext: SQLContext) = {
    val schema = StructType(StructField("byte", ByteType, true)::Nil)
    val inputData = Seq(
      "{'byte': 'a'}",
      "{'byte': 'b'}",
      "{'byte': 1}",
      "{'byte': 0}",
      "{'byte': 5}",
      "{'byte': null}")

    val rdd = sc.parallelize(inputData)

    Printer.PrintDataExample(sqlContext, "ByteType", rdd, schema, inputData.mkString("\n"))
  }
}
