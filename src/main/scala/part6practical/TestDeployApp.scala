package part6practical

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

case object TestDeployApp {
  def main(args: Array[String]): Unit = {
    /**
     * Let's assume we have movies.json as args(0)
     * GoodComedies.json as args(1)
     *
     * good comedy = genre === Comedy and IMDB>6.5
     */

    if (args.length!=2) {
      println("Need input path and output path!")
      System.exit(1)
    }

      val spark = SparkSession.builder()
        .appName("test Deploy App")
      // we don't configure the spark master to be local; we'll pass that as command line arguments to Spark submit later on
        .getOrCreate()

      val moviesDF = spark
        .read
        .option("inferSchema", "true")
        .json(args(0))

      val goodComediesDF = moviesDF.select(
        col("Title"),
        col("IMDB_Rating").as("Rating"),
        col("Release_Date").as("Release")
      )
        .filter(col("Major_Genre")==="Comedy" and col("IMDB_Rating")>6.5)
        .orderBy(col("Rating").desc_nulls_last)

    goodComediesDF.show()

    // now we're going to write third DF into the file denoted by args(1)
    goodComediesDF.write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save(args(1))
  }
}
