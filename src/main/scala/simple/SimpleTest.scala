import org.apache.spark.sql.SparkSession

object SimpleTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SimpleTest")
      .master("spark://192.168.0.107:7077")
      .getOrCreate()

    println("=" * 60)
    println("CLUSTER CONNECTION SUCCESSFUL!")
    println("=" * 60)
    println(s"Spark version: ${spark.version}")
    println(s"Master URL: ${spark.sparkContext.master}")
    println(s"Application ID: ${spark.sparkContext.applicationId}")
    println("=" * 60)
    
    spark.stop()
  }
}
