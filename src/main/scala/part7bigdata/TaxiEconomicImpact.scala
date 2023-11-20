package part7bigdata

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._


object TaxiEconomicImpact extends App {

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Taxi Big Data Application")
    .getOrCreate()

  import spark.implicits._

  val bigTaxiDF = spark.read.load("src/main/resources/data/NYC_taxi_2009-2016.parquet")
  val taxiZonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")


  val percentGroupAttempt = 0.05
  val percentAcceptGrouping = 0.3
  val discount = 5
  val extraCost = 2
  val avgCostReduction = 0.6 * bigTaxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0)
  val percentGroupable = 289623 * 1.0 / 331893

  val groupAttemptsDF = bigTaxiDF
    .select(
      round(unix_timestamp(col("pickup_datetime")) / 300).cast("integer").as("fiveMinId"),
      col("pickup_taxizone_id"),
      col("total_amount")
    )
    .groupBy(col("fiveMinId"), col("pickup_taxizone_id"))
    .agg((count("*")*percentGroupable).as("total_trips"), sum(col("total_amount")).as("total_amount"))
    //    .orderBy(col("total_trips").desc_nulls_last)
    .withColumn("approximate_datetime", from_unixtime(col("fiveMinId") * 300))
    .drop("fiveMinId")
    .join(taxiZonesDF, col("pickup_taxizone_id") === col("LocationID"))
    .drop("LocationID", "service_zone")
    .orderBy(col("total_trips").desc_nulls_last)

  groupAttemptsDF.show()

  val groupingEstimateEconomicImpactDF = groupAttemptsDF
    .withColumn("groupedRides", col("total_trips") * percentGroupAttempt)
    .withColumn("acceptedGroupedRidesEconomicImpact", col("groupedRides") * percentAcceptGrouping * (avgCostReduction - discount))
    .withColumn("rejectedGroupedRidesEconomicImpact", col("groupedRides") * (1 - percentAcceptGrouping) * extraCost)
    .withColumn("totalImpact", col("acceptedGroupedRidesEconomicImpact") + col("rejectedGroupedRidesEconomicImpact"))

  groupingEstimateEconomicImpactDF.show(100)

  val totalProfitDF = groupingEstimateEconomicImpactDF.select(sum(col("totalImpact")).as("total"))
  totalProfitDF.show()

}
