ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.16"

val sparkVersion = "4.0.1"
val parquet4sVersion = "2.17.0"

lazy val root = (project in file("."))
  .settings(
    name := "graph-classifier-scala",

    libraryDependencies ++= Seq(
      // Spark dependencies (for Spark-based implementation)
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,

      // Parquet4s - pure Scala Parquet library (replaces Hadoop-based parquet)
      "com.github.mjakubowski84" %% "parquet4s-core" % parquet4sVersion,

      // Scala parallel collections
      "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4"
    ),

    // Compiler options
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xlint",
      "-Ywarn-dead-code",
      "-Ywarn-unused",
      "-encoding", "utf8"
    ),

    // Fork the JVM for running
    fork := true,
    javaOptions ++= Seq(
      "-Xms512M",
      "-Xmx4G",
      "-XX:+UseG1GC",
      "-Dlog4j2.configurationFile=log4j2.properties"
    )
  )
