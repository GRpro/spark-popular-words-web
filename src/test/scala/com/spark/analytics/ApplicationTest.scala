package com.spark.analytics

import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.FunSuite


class ApplicationTest extends FunSuite {

  test("top words should be calculated correctly") {

    implicit val sparkSession = LocalRunner.createLocalSession()
    import sparkSession.implicits._

    def source = List(
     "ScAlA, scala , java , sbt \n maven ;   scala. Java",
     "java, git,Scala ... Maven, sbt, SCALA, ",
     "Java, maven"
    ).toDF("text")

    def sink(result: DataFrame): Unit = {
      assert(Array(Row("scala", 5), Row("java",  4), Row("maven", 3), Row("sbt", 2), Row("git", 1)) === result.collect())
    }

    val job = new Application(source, sink, 40)
    job.run
  }

}
