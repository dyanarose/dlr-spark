import Dependencies._

lazy val commonSettings = Seq(
  organization := "dlr",
  version := "1.0",
  scalaVersion := "2.10.5"
)

lazy val dataTypes = (project in file("data-types"))
  .settings(commonSettings: _*)
  .settings(name := "data-types")
  .settings(libraryDependencies ++= basicDependencies)
  .settings(mainClass in (Compile, run)  := Some("dlr.datatypes.Output"))