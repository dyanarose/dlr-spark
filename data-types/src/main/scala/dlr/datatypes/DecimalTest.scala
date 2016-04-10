package dlr.datatypes

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{DecimalType, DoubleType, StructField, StructType}

/**
  * Created by dyana on 10/04/16.
  */
object DecimalTest {
  def RunTest(sc: SparkContext, sqlContext: SQLContext) = {
    // Will error when the number is too large to satisfy both the scale and the precision
    // When knowing the precision required is not possible, use max?
    val schema = StructType(StructField("decimal", DecimalType(6, 3), true)::Nil)
    val posInf = Double.PositiveInfinity
    val negInf = Double.NegativeInfinity
    val nan = Double.NaN
    val inputData = Seq(
      "{'decimal': 1.2345}",
      "{'decimal': 1}",
      "{'decimal': 234.231}",
      s"{'decimal': $posInf}",
      s"{'decimal': $negInf}",
      s"{'decimal': $nan}",
      "{'decimal': '1'}",
      "{'decimal': '1.2345'}",
      "{'decimal': null}")

    val rdd = sc.parallelize(inputData)

    Printer.PrintDataExample(sqlContext, "DecimalType", rdd, schema, inputData.mkString("\n"))
  }
}
