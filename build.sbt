ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "newday-data-eng"
  )

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies +="org.apache.spark" %% "spark-sql" % "3.3.0"
