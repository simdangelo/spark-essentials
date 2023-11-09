package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr}

object Joins extends App{

  val spark = SparkSession.builder
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // joins
  val joinCondition = guitaristsDF.col("band")===bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")

  // outer joins
  // left outer: everything in the inner join + all the rows in the LEFT table, with nulls in there the data is missing
    guitaristsDF.join(bandsDF, joinCondition, "left_outer")

  // right outer: everything in the inner join + all the rows in the RIGHT table, with nulls in there the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")

  // full outer: everything in the inner join + all the rows in the BOTH table, with nulls in there the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "outer")

  // semi-joins:
  // left_semi: it's like an inner join but cut out the columns from the RIGHT dataframe
  guitaristsDF.join(bandsDF, joinCondition, "left_semi")

  // anti-join
  // keep the rows in the LEFT dataframe for which there is no row RIGHT dataframe satisfying the join condition
  guitaristsDF.join(bandsDF, joinCondition, "left_anti").show()

  // things to bear in mind
  // 1. the following code will crash guitaristsBandsDF.select("id", "band").show() because guitaristsBandsDF has two "id" columns
  //  guitaristsBandsDF.select("id", "band").show() -> this crashes
  //  Solution:
  // - option 1 - rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // - option 2 - drop the duplicate column
  guitaristsBandsDF.drop(bandsDF.col("id"))

  // - option 3 - rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsBandsDF.join(bandsModDF, guitaristsDF.col("band")===bandsModDF.col("bandId"))

  // using complex types
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarID)")).show()

}
