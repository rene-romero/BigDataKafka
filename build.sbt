ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

lazy val root = (project in file("."))
  .settings(
    name := "BigDataKafka"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.13" % "3.3.2",
  "org.apache.kafka" % "kafka-clients" % "3.4.0",
  "org.postgresql" % "postgresql" % "42.5.4",
  "com.typesafe" % "config" % "1.4.2",
  "org.slf4j" % "slf4j-simple" % "2.0.7",
  "mysql" % "mysql-connector-java" % "8.0.33"
)