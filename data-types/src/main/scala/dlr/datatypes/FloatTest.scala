package dlr.datatypes

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{FloatType, StructField, StructType}

/**
  * Created by dyana on 10/04/16.
  */
object FloatTest {
  def RunTest(sc: SparkContext, sqlContext: SQLContext) = {
    val schema = StructType(StructField("float", FloatType, true)::Nil)
    val max = Float.MaxValue
    val posInf = Float.PositiveInfinity
    val negInf = Float.NegativeInfinity
    val nan = Float.NaN
    val min = Float.MinValue
    val inputData = Seq(
      "{'float': 1.23456}",
      "{'float': 1}",
      s"{'float': $max}",
      s"{'float': $min}",
      s"{'float': $posInf}",
      s"{'float': $negInf}",
      s"{'float': $nan}",
      "{'float': '1'}",
      "{'float': '1.23456'}",
      "{'float': null}")

    val rdd = sc.parallelize(inputData)

    Printer.PrintDataExample(sqlContext, "FloatType", rdd, schema, inputData.mkString("\n"))
  }
}
