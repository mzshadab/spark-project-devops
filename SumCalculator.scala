import org.apache.spark.sql.SparkSession

object SumCalculator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SumCalculator")
      .getOrCreate()

    import spark.implicits._

    val numbers = Seq(1, 2, 3, 4, 5)
    val numbersDF = numbers.toDF("number")
    val sum = numbersDF.selectExpr("sum(number) as total")

    sum.show()

    spark.stop()
  }
}
