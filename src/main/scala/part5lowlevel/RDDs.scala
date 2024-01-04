package part5lowlevel

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

object RDDs extends App{

  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  // several ways to create an RDD

  // 1 - Parallelize an existing collection
  val numbers = 1 to 1000000
  val numbersRDD = sc.parallelize(numbers)

  // 2 - reading from files
  case class StockValue(symbol: String, date: String, price: Double)
  def readStocks(filename: String) =
    Source.fromFile(filename)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2b - reading from files - alternative solution
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0)) // to eliminate the header
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - read from a DF
  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._
  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd // the type information is preserved

  val stockRDD4 = stocksDF.rdd // the type information is lost

  // conversion RDD -> DF
  val numbersDF = numbers.toDF("numbers") // you lose the type information

  // conversion RDD -> DS
  val numbersDS = spark.createDataset(numbersRDD)


  // Transformations
  // counting
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT") // lazy transformation
  val msCount = msftRDD.count() // action

  // distinct
  val companyNamesRDD = stocksRDD.map(_.symbol).distinct() // also lazy

  // min and max
  implicit val stockOrdering: Ordering[StockValue] = Ordering.fromLessThan((sa, sb) => sa.price < sb.price)
  val minMsft = msftRDD.min()

  // reduce
  numbersRDD.reduce(_ + _)

  // grouping
  val groupedStockRDD = stocksRDD.groupBy(_.symbol)
  // very expensive - involves shuffling

  // partitioning
  val repartitionedStocksRDD = stocksRDD.repartition(30)
  repartitionedStocksRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")

  // coalesce
  val coalesceRDD = repartitionedStocksRDD.coalesce(15) // does not involve shuffling
  coalesceRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks15")


  /**
   * Exercises
   *
   * 1 - Read the movie.json as an RDD of the case class specified below
   * 2 - Show the distinct genres as an RDD
   * 3 - Select all the movies in the Drama genre with IMDB rating > 6
   * 4 - Show the average rating of movies by genre
   */

  // Exercise1
  case class Movie(title: String, genre: String, rating: Double)
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val moviesRDD = moviesDF
    .select(col("Title").as("title"), col("Major_genre").as("genre"), col("IMDB_Rating").as("rating"))
    .where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd

  // Exercise2
  val genresRDD = moviesRDD.map(_.genre).distinct()

  // Exercise3
  val goodDramasRDD = moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6)

  // Exercise4
  case class GenreAvgRating(genre: String, rating: Double)
  val avgRatingByGenreRDD = moviesRDD
    .groupBy(_.genre).map {
      case (genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
    }

  avgRatingByGenreRDD.toDF().show()
  moviesRDD.toDF().groupBy(col("genre")).avg("rating").show()

}
