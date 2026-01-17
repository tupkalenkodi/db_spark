package main

import com.sun.management.OperatingSystemMXBean
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.lang.management.ManagementFactory
import java.nio.file.{Files, Paths}


object SparkSessionFactory {

  private def checkSystemResources(): (Long, Long, Int) = {
    val osBean = ManagementFactory.getOperatingSystemMXBean
      .asInstanceOf[OperatingSystemMXBean]

    val totalMemory = osBean.getTotalMemorySize / (1024 * 1024 * 1024)
    val freeMemory = osBean.getFreeMemorySize / (1024 * 1024 * 1024)
    val logicalCores = osBean.getAvailableProcessors

    (totalMemory, freeMemory, logicalCores)
  }

  def createSession(appName: String): SparkSession = {
    val (totalMemory, availableMemory, cpuLogical) = checkSystemResources()

    val usableMemory = (availableMemory * 0.9).toInt
    val driverMemory = Math.max(2, (usableMemory * 0.7).toInt)
    val executorMemory = Math.max(1, (usableMemory * 0.3).toInt)

    val executorCores = Math.max(1, cpuLogical - 1)

    val parallelism = executorCores * 2

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")
      // Memory settings
      .set("spark.driver.memory", s"${driverMemory}g")
      .set("spark.executor.memory", s"${executorMemory}g")
      .set("spark.executor.cores", executorCores.toString)
      // Memory management
      .set("spark.memory.fraction", "1")
      // Performance optimizations
      .set("spark.sql.files.maxPartitionBytes", "134217728")
      .set("spark.default.parallelism", parallelism.toString)
      .set("spark.sql.shuffle.partitions", parallelism.toString)
      // Adaptive query execution
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .set("spark.sql.adaptive.skewJoin.enabled", "true")
      .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
      // Serialization
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      // SQL optimizations
      .set("spark.sql.autoBroadcastJoinThreshold", "10485760")
      .set("spark.sql.broadcastTimeout", "300")
      .set("spark.sql.inMemoryColumnarStorage.compressed", "true")
      // Parquet optimizations
      .set("spark.sql.parquet.enableVectorizedReader", "true")
      .set("spark.sql.parquet.columnarReaderBatchSize", "4096")

    // Create Spark session
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")
    val checkpointDir = "/tmp/spark-checkpoint"
    val checkpointPath = Paths.get(checkpointDir)

    if (!Files.exists(checkpointPath)) {
      Files.createDirectories(checkpointPath)
    }

    spark.sparkContext.setCheckpointDir(checkpointDir)

    spark
  }

  def stopSession(spark: SparkSession): Unit = {
    try {
      spark.stop()
    } catch {
      case e: Exception =>
        println(s"Error stopping Spark session: ${e.getMessage}")
    }
  }
}