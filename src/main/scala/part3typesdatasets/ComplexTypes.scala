package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, col, current_date, current_timestamp, datediff, expr, struct, to_date, split, size}

case object ComplexTypes extends App {
  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")


  // DATES
  val moviesWithReleaseDate = moviesDF.
    select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release")) // conversion

  moviesWithReleaseDate
    .withColumn("Today", current_date()) // today
    .withColumn("Right_Now", current_timestamp()) // this second
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release"))/365)  // date_add, date_sub
    .show()

  moviesWithReleaseDate.select("*").filter(col("Actual_Release").isNull).show()

  /**
   * Exercise
   * 1. how do we deal with multiple formats?
   * 2. Read the stocks DF and parse the date correctly
   */

  // EXERCISE 1
  // parse the data multiple time, then union the small DFs.
  // - if your DF is really big, parse it multiple times could not be feasible
  // - alternative: if you have an irrelevant number of records with an incorrect format, you can ignore them


  // EXERCISE 2
  val stocksDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")

  val stocksWithDate = stocksDF.select("*")
    .withColumn("Actual_Date", to_date(col("date"), "MMM d yyyy"))


  // STRUCTURES
  //  Structures in Spark are groups of columns aggregates into one

  // 1 - with col operator
  moviesDF
    .select(
      col("Title"),
      struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"),
      col("Profit").getField("Us_Gross").as("Us_Gross_extracted")
    )
    .show()

  // 2 - with expression strings
  moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")


  // arrays
  val moviesWithWords = moviesDF.select(col("Title"), split(col("title"), " |,").as("Title_Words")) // ARRAY of strings

  moviesWithWords.select(
    col("Title"),
    col("Title_Words"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love")
  ).show()


}
