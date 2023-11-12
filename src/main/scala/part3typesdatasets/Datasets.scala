package part3typesdatasets

import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Date

object Datasets extends App {
  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF: DataFrame = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()
  numbersDF.show()

  // convert DF to DS
  implicit val intEncoders = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  numbersDS.filter(_ < 100)


  // DATASET of a complex type
  val carsSchema = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", DateType),
      StructField("Origin", StringType)
    )
  )

  def readDF(filename: String, schema: StructType) =
    spark.read
      .option("inferSchema", "true")
      .schema(schema)
      .json(s"src/main/resources/data/$filename")

  // 1 - define your case class
  case class Car(
                  Name: String,
                  Miles_Per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: Date,
                  Origin: String,
                )

  // 2 - read the DF from the file
  val carsDF = readDF("cars.json", carsSchema)

  // 3 - define an encoder (importing the implicits)
  import spark.implicits._

  // 4 - convert the DF to DS
  val carsDS = carsDF.as[Car]

  // DS collection functions
  // example1
  numbersDS.filter(_ < 100).show()
  // example2 - you can also use map,flatMap, fold, reduce, for comprehensions ...
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())

  carNamesDS.show()

  /**
   * Exercises
   *
   * 1. count how many cars we have
   * 2. count how may powerful cars we have (powerful= horsepower>140)
   * 3. average HP for the entire dataset
   */

  // Exercise1
  println(carsDS.count)

  // Exercise2
  //  carsDS.filter(_.Horsepower>140) --> // this returns an error by the compiler because Horsepower is defined as
  //  an Option in the case class Car.
  //  So you need to modify in the following way in order to get the value 0 where we have a Null:
  println(carsDS.filter(_.Horsepower.getOrElse(0L)>140).count()) // we add "L" because Horsepower is a Long, but "0" is int and there is a type mismatch

  // Exercise3
  //solution1
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsDS.count())

  // solution 2 - also use the DF functions
  carsDS.select(avg(col("Horsepower"))).show()

  // Actually, solution1 and solution2 returns two different results: Daniels said it's because of Nulls
  // (one solution treats them as 0, the other ignores them), but an user got different results
  // even with Nulls management (he add a coalesce to makes the 2 solutions equals)
}
