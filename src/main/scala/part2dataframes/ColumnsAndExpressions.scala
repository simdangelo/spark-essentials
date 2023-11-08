package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_remove, col, column, expr}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("Src/main/resources/data/cars.json")

  // columns
  val firstColumn = carsDF.col("Name")

  // selecting
  val carNamesDF = carsDF.select(firstColumn)


  // various select methods

  import spark.implicits._

  carsDF.select(
    carsDF.col("Name"),
    // alternative1
    col("Name"),
    // alternative2
    column("Name"),
    //    'Year, Scala Symbol, auto-converted to column
    $"Horsepower", // fancier interpolated string, returns a Column Object
    expr("Origin") //expression
  )

  // select with plain column names
  carsDF.select("Name", "Year")

  // EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpressions = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWightDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpressions.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  val carsWithSelectExprWeightsDF = carsDF
    .selectExpr(
      "Name",
      "Weight_in_lbs",
      "Weight_in_lbs / 2.2"
    )

  // DF processing
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // careful with column names
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")
  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")
  // filtering with expressions string
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter((col("Origin") === "USA").and(col("Horsepower") > 150))
  val americanPowerfulCarsDF2_2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  /// unioning = adding moew rows
  val moreCarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have the same schema

  // distinct values
  val allCountriesDF = carsDF.select(col("Origin")).distinct()
  allCountriesDF.show()

  /**
   * EXERCISES
   *
   * 1. read the movies DF and select 2 columns of your choice
   * 2. create a new column summing up the total profit of the movie
   * 3. select all COMEDY movies with IMDB raring above 6
   *
   * Use as many version as possible
   */

  // Exercise1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val firstTwoColumns = moviesDF.select("Title", "US_Gross")
  val firstTwoColumns2 = moviesDF.select(
    moviesDF.col("Title"),
    col("US_Gross"),
    $"Major_Genre",
    expr("IMDB_Rating")
  )

  val firstTwoColumns3 = moviesDF.selectExpr(
    "Title", "US_Gross"
  )

  // Exercise2
  // first option
  val moviesProfit = moviesDF.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    (col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross")
  )

  // second option
  val moviesProfit2 = moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_Gross + Worldwide_Gross as Total_Gross"
  )

  // third option
  val moviesProfit3 = moviesDF.select("Title", "US_Gross", "Worldwide_Gross")
    .withColumn("Total_Gross", col("US_Gross") + col("Worldwide_Gross"))

  // Exercise3
  // first option
  val atLeastMediocreComediesDF = moviesDF.select("Title", "IMDB_Rating")
    .filter(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)

  // second option
  val atLeastMediocreComediesDF2 = moviesDF.select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy")
    .where(col("IMDB_Rating") > 6)

  // third option
  val atLeastMediocreComediesDF3 = moviesDF.select("Title", "IMDB_Rating")
    .where("Major_Genre = 'Comedy' and IMDB_Rating > 6")

}
