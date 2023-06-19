name := "MyProject"

version := "1.0"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "org.apache.spark" %% "spark-streaming" % "3.0.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.1"
  // Add other dependencies as needed
)