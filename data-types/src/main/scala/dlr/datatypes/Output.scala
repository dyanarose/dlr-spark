package dlr.datatypes

import java.io.FileOutputStream

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by dyana on 09/04/16.
  */
object Output {
  def main(args: Array[String]) = {
    // See:
    // https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/literals.scala
    // for some details on how the types are converted from json

    Console.setOut(new FileOutputStream(args(0), true))
    val conf = new SparkConf().setMaster("local[*]").setAppName("data types")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    DecimalTest.RunTest(sc, sqlContext)
    BooleanTest.RunTest(sc, sqlContext)
    ByteTest.RunTest(sc, sqlContext)
    CalendarIntervalTest.RunTest(sc, sqlContext)
    DateTest.RunTest(sc, sqlContext)
    DoubleTest.RunTest(sc, sqlContext)
    FloatTest.RunTest(sc, sqlContext)
    IntegerTest.RunTest(sc, sqlContext)
    LongTest.RunTest(sc, sqlContext)
    MapTest.RunTest(sc, sqlContext)
    NullTest.RunTest(sc, sqlContext)
    ShortTest.RunTest(sc, sqlContext)
    TimestampTest.RunTest(sc, sqlContext)

    sc.stop
  }
}
