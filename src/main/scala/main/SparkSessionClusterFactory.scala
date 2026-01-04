package main

import org.apache.spark.sql.SparkSession

object SparkSessionClusterFactory {

    def createSession(appName: String): SparkSession = {
        val spark = SparkSession.builder()
          .appName(appName)
          .master("spark://192.168.0.107:7077")  // Your cluster master
          .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        // Checkpoint directory
        spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoint")

        println("=" * 70)
        println(s"SPARK CLUSTER SESSION CREATED")
        println("=" * 70)
        println(s"Application: $appName")
        println(s"Master URL: spark://192.168.0.107:7077")
        println("=" * 70)

        spark
    }

    def stopSession(spark: SparkSession): Unit = {
        try {
            spark.stop()
            println("Spark session stopped successfully")
        } catch {
            case e: Exception =>
                println(s"Error stopping Spark session: ${e.getMessage}")
        }
    }
}