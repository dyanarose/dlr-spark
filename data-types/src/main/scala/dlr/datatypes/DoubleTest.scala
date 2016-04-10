package dlr.datatypes

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

/**
  * Created by dyana on 10/04/16.
  */
object DoubleTest {
  def RunTest(sc: SparkContext, sqlContext: SQLContext) = {
    val schema = StructType(StructField("double", DoubleType, true)::Nil)
    val max = Double.MaxValue
    val posInf = Double.PositiveInfinity
    val negInf = Double.NegativeInfinity
    val nan = Double.NaN
    val min = Double.MinValue
    val inputData = Seq(
      "{'double': 1.23456}",
      "{'double': 1}",
      s"{'double': $max}",
      s"{'double': $min}",
      s"{'double': $posInf}",
      s"{'double': $negInf}",
      s"{'double': $nan}",
      "{'double': '1'}",
      "{'double': '1.23456'}",
      "{'double': null}")

    val rdd = sc.parallelize(inputData)

    Printer.PrintDataExample(sqlContext, "DoubleType", rdd, schema, inputData.mkString("\n"))
  }
}
