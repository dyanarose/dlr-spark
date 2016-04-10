import sbt.Keys._
import sbt._

object Dependencies {
  val sparkCore = "org.apache.spark" %% "spark-core" % "1.6.1"
  val sparkSql = "org.apache.spark" %% "spark-sql" % "1.6.1"

  val basicDependencies = Seq(sparkCore, sparkSql)
}