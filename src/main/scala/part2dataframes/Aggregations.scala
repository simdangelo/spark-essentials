package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App{
  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // counting
  moviesDF.select(count(col("Major_Genre"))) // all the values except null
  moviesDF.selectExpr("count(Major_Genre)") // alternative

  // count all
  moviesDF.select(count("*")) // count all the rows, and will include nulls

  // counting distinct
  moviesDF.select(countDistinct(col("Major_Genre")))

  // approximate count: useful for large dataset to quick analyze because it will not scan the entire df row by row, but just an approximation
  moviesDF.select(approx_count_distinct(col("Major_Genre")))

  // min & max
  moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDF.select(sum(col("US_Gross")))
  moviesDF.selectExpr("sum(US_Gross)")

  //avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_rating")),
    stddev(col("Rotten_Tomatoes_rating"))
  )

  // grouping
  val countByGenreDF = moviesDF.groupBy(col("Major_Genre")) // it includes null
    .count() // equivalent of SQL command: select count(*) from table1 group by Major_Genre

  val avgratingByGenreDF = moviesDF.groupBy("Major_Genre")
    .avg("IMDB_Rating")

  // another way to do aggregation is with .agg
  val aggregationByGenreDF = moviesDF.groupBy("Major_Genre")
    .agg(
      count("*").as("N_movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_rating"))

  /**
   * EXERCISES
   *
   * 1. Sum up ALL the profits of ALL the movies in the DF
   * 2. Count how many distinct directors we have
   * 3. Show the mean and the standard deviation of the US gross revenue for the movies
   * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
   */

  // Exercise1
  moviesDF.select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
    .select(sum("Total_Gross"))
    .show()

  // Exercise2
  moviesDF.select(countDistinct(col("Director"))).show()

  // Exercise3
  moviesDF.select(
    mean("US_Gross"),
    stddev("US_Gross")
  ).show()

  // Exercise4
  moviesDF.groupBy("Director").agg(
    avg("IMDB_Rating").as("Avg_rating"),
    sum("US_Gross").as("Total_US_Gross")
  )
    .orderBy(col("Avg_Rating").desc_nulls_last)
    .show()
}
