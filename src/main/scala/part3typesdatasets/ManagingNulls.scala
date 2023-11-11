package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col}

object ManagingNulls extends App {
  val spark = SparkSession.builder()
    .appName("Managing Nulls")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // select the first non-null value
  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_rating"), col("IMDB_Rating")*10)
  )

  // checking for nulls
  moviesDF.select("*").filter(col("Rotten_Tomatoes_rating").isNull)

  // nulls when ordering
  moviesDF.orderBy(col("IMDB_rating").desc_nulls_last)

  // remove nulls
  moviesDF.select("Title", "IMDB_Rating").na.drop() // remove rows containing nulls

  // replace nulls
  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating"))
  moviesDF.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_rating" -> 10,
    "Director" -> "Unknown"
  )).show()

  // complex orperation
  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_rating",
    "ifnull(Rotten_Tomatoes_rating, IMDB_Rating * 10) as ifnull", // same as coalesce
    "nvl(Rotten_Tomatoes_rating, IMDB_Rating * 10) as nvl", // same
    "nullif(Rotten_Tomatoes_rating, IMDB_Rating * 10) as nullif", // returns null if the 2 values are equal, else first value
    "nvl2(Rotten_Tomatoes_rating, IMDB_Rating * 10, 0.0) as nvl2" // if (first!=null) second else third
  )
    .show()

}