name := "healthCareScala"

version := "0.1"
scalaVersion := "2.11.12"


// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0"
)

