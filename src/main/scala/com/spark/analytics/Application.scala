package com.spark.analytics

import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.jsoup.Jsoup

object Application {

  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("Spark web scrapper")
      .getOrCreate
  }

  val extractText = udf { (uri: String) => Jsoup.connect(uri).get().text() }
  val flattenWords = udf { (words: Seq[String]) => words}

  def main(args: Array[String]): Unit = {
    val inputFilePath = "input/input.txt"
    val destPath = "output"
    val n = 10




    lazy val sparkSession: SparkSession = createSparkSession()
    import sparkSession.implicits._

    val uris = sparkSession.sparkContext.textFile(inputFilePath)
      .toDF("uri")

    val text = uris.withColumn("text", extractText(col("uri")))

    /*
    [^\\w]+ - one or more non-alphanumeric-character
    (\\d\\w*)* - which can be followed by zero or more words which start with digit
    ([^\\w]+(\\d\\w*)*)+ - delimiter can repeat more than once (this way we will avoid returning empty strings between delimiters)
     */
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("text")
      .setOutputCol("words")
      .setPattern("([^\\w]+(\\d\\w*)*)+")
      .setToLowercase(true)

    val remover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filtered_words")

    val tokenized = regexTokenizer.transform(text).select(col("uri"), col("words"))
    val cleaned = remover.transform(tokenized).select(col("uri"), col("filtered_words"))

    val counts = cleaned.select("filtered_words").withColumn("words", explode(col("filtered_words")))

    counts.show()

    val result = counts.groupBy("words").count().orderBy(desc("count"))
    result.collect().foreach(println(_))

    result.write.format("com.databricks.spark.csv").option("header","true").save(destPath)
  }
}
