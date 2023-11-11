package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App {
  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // adding a plain value to a DF
  moviesDF.select(col("Title"), lit(47).as("plain_value"))


  // BOOLEANS
  val dramaFilter = col("Major_Genre")==="Drama"
  val goodRaringFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRaringFilter

  moviesDF.select("Title").where(preferredFilter)
  // + multiple ways of filtering

  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movies"))
  // filter on a boolean column
  moviesWithGoodnessFlagsDF.filter("good_movies") // filter(col("good_movie")==="true")

  // negations
  moviesWithGoodnessFlagsDF.filter(not(col("good_movies")))


  // NUMBERS
  // math operators
  val moviesAvgRatingDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_rating")/10 + col("IMDB_Rating"))/2)

  // correlation (useful for data science)
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")) // corr is an ACTION


  // STRINGS
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // capitalization: initcap, lower, upper
  carsDF.select(initcap(col("Name"))) // capitalize the first letter of every words

  // contains
  carsDF.select("*").filter(col("Name").contains("volkswagen"))

  // regex
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  )
    .filter(col("regex_extract")=!="")

  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's car").as("regex_replace")
  ).show()

  /**
   * Exercise
   *
   * Filter the car fg by the list of car names obtained by an API call
   */

  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  // version 1 - using regex
  val complexRegex = getCarNames.map(_.toLowerCase()).mkString("|") // this returns volkswagen|mercedes-benz|ford
  carsDF.select(
      col("Name"),
      regexp_extract(col("Name"), complexRegex, 0).as("regex_extract")
    )
    .filter(col("regex_extract") =!= "")
    .show()

  // version 2 - using contains

  var carNameFilters = getCarNames.map(_.toLowerCase()).map(name => col("Name").contains(name))
  var bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
  carsDF.filter(bigFilter).show()

}
