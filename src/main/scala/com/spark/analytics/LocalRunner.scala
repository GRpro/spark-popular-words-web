package com.spark.analytics

import org.apache.spark.sql.SparkSession

object LocalRunner {

  val DEFAULT_N = 10

  def createLocalSession(): SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("Spark web scrapper")
      .getOrCreate
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 2 || args.length > 3) {
      System.err.println("Wrong parameters. Usage: LocalRunner <input-uri> <output-path> [n]")
      System.exit(1)
    }

    val inputUri: String = args(0)
    val outputPath: String = args(1)
    val n: Int = if (args.length == 3) args(2).toInt else DEFAULT_N


    implicit val sparkSession = createLocalSession()

    val application = Application(inputUri, outputPath, n)

    application.run
  }
}
