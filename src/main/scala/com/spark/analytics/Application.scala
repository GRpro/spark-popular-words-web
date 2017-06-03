package com.spark.analytics

import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.jsoup.Jsoup

import scala.util.Try


class Application(source: => DataFrame,
                  sink: DataFrame => Unit,
                  val topN: Int) {

  def run(implicit sparkSession: SparkSession): Unit = {
    val text = source

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

    val tokenized = regexTokenizer.transform(text).select(col("words"))
    val cleaned = remover.transform(tokenized).select(col("filtered_words"))
    val counts = cleaned.select("filtered_words").withColumn("words", explode(col("filtered_words")))
    val result = counts.groupBy("words").count().orderBy(desc("count")).limit(topN)

    sink(result)
  }
}

object Application {

  def webSource(inputUri: String)(implicit sparkSession: SparkSession) = {

    import sparkSession.implicits._

    // bad uris will be filtered
    val extractText = udf { (uri: String) => Try(Jsoup.connect(uri).get().text()).getOrElse(null) }

    val text = sparkSession.sparkContext.textFile(inputUri)
      .toDF("uri")
      .withColumn("text", extractText(col("uri")))
      .filter($"text".isNotNull)
    text
  }

  def fileSink(outputUri: String)(result: DataFrame)(implicit sparkSession: SparkSession) = {
    result.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(outputUri)
  }

  def apply(inputUri: String, outputUri: String, topN: Int)(implicit sparkSession: SparkSession): Application =
    new Application(webSource(inputUri), fileSink(outputUri), topN)
}