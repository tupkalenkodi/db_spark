package test

import org.apache.spark.sql.SparkSession

object ClusterTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("ClusterTest")
      .master("spark://192.168.0.107:7077")
      .config("spark.driver.host", "192.168.0.107")
      .config("spark.driver.port", "7078")
      .config("spark.blockManager.port", "7079")
      .getOrCreate()

    println("=" * 60)
    println("CLUSTER CONNECTION TEST")
    println("=" * 60)
    
    println(s"Spark Version: ${spark.version}")
    println(s"Master URL: ${spark.sparkContext.master}")
    println(s"Application ID: ${spark.sparkContext.applicationId}")
    println(s"Default Parallelism: ${spark.sparkContext.defaultParallelism}")
    
    // Test distributed computation
    val data = spark.sparkContext.parallelize(1 to 1000, 10)
    val sum = data.sum()
    println(s"Distributed sum of 1 to 1000: $sum")
    
    spark.stop()
    println("Test completed successfully!")
  }
}
