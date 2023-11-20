package part7bigdata

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{min, _}

object TaxiApplications extends App {

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Taxi Big Data Application")
    .getOrCreate()

  import spark.implicits._

  val taxiDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  taxiDF.printSchema()
  println(taxiDF.count())
  taxiDF.show(5)

  val taxiZonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")
  taxiZonesDF.printSchema()
  taxiZonesDF.show()

  /**
   * QUESTIONS:
   * 1. Which zones have the most pickups/dropoffs overall?
   */

  // QUESTION1. Which zones have the most pickups/dropoffs overall?
  val pickupsByTaxiZoneDF = taxiDF
    .groupBy("PULocationID")
    .agg(count("*").alias("totalTrips"))
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("LocationID", "service_zone")
    .sort(col("totalTrips").desc_nulls_last)

  //  pickupsByTaxiZoneDF.show()

  // QUESTION1b. let's group by Borough to see if Manhattan wins and if so, how much is more popular
  val pickupsByBorrowDF = pickupsByTaxiZoneDF
    .groupBy("Borough")
    .agg(sum(col("totalTrips")).as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)

  //    pickupsByBorrowDF.show()

  // QUESTION2. What are he peak hours for taxi?
  val pickupsByHourDF = taxiDF
    .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy("hour_of_day")
    .agg(count("*").alias("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)

  //  pickupsByHourDF.show()


  // QUESTION3. How are the trips distributed by length? Why are people taking the cab?
  val tripDistanceDF = taxiDF
    .select(col("trip_distance").as("distance"))
  val longDistanceThreshold = 30
  val tripDistanceStatsDF = tripDistanceDF
    .select(
      count("*").as("count"),
      lit(longDistanceThreshold).as("threshold"),
      mean("distance").as("mean"),
      stddev("distance").as("stddev"),
      min("distance").as("min"),
      max("distance").as("max")
    )

  //  tripDistanceStatsDF.show()

  val tripsWithLengthDF = taxiDF.withColumn("isLong", col("trip_distance") >= longDistanceThreshold)
  val tripsByLengthDF = tripsWithLengthDF
    .groupBy("isLong").count()

  //  tripsByLengthDF.show()
  // since we obtained only 83 records as "Long" trips, it wouldn't worth to explore question4 and 5 for long trips, but we'll do anyway


  //  QUESTION4. What are the peak hours for long/short trips?
  val pickupsByHourByLengthDF = tripsWithLengthDF
    .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy("hour_of_day", "isLong")
    .agg(count("*").alias("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)

  //  pickupsByHourByLengthDF.show(48)


  //  QUESTION5. What are the top3 pickup/dropoff zones for long/short trips?
  def pickupDropoffpopularity(predicate: Column) = tripsWithLengthDF
    .filter(predicate)
    .groupBy("PULocationID", "DOLocationID")
    .agg(count("*").as("totalTrips"))
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .withColumnRenamed("Zone", "Pickup_Zone")
    .drop("LocationID", "Borough", "service_zone")
    .join(taxiZonesDF, col("DOLocationID") === col("LocationID"))
    .withColumnRenamed("Zone", "Dropoff_Zone")
    .drop("LocationID", "Borough", "service_zone")
    .drop("PULocationID", "DOLocationID")
    .orderBy(col("totalTrips").desc_nulls_last)

  val pickupDropoffPopularityLongDF = pickupDropoffpopularity(col("isLong"))
  val pickupDropoffPopularityShortDF = pickupDropoffpopularity(not(col("isLong")))

  //  pickupDropoffPopularityLongDF.show()
  //  pickupDropoffPopularityShortDF.show()

  //  QUESTION6. How are people paying for the ride, on long/short trips?
  val ratecodeDistributionDF = taxiDF
    .groupBy(col("RatecodeID"))
    .agg(count("*").as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)

  //  ratecodeDistributionDF.show()


  //  QUESTION7. How is the payment type evolving with time?
  val ratecodeEvolutionDF = taxiDF
    .groupBy(to_date(col("tpep_pickup_datetime")).as("pickup_day"), col("RatecodeID"))
    .agg(count("*").as("totalTrips"))
    .orderBy(col("pickup_day"))


  // QUESTION8. Can we explore a ride-sharing opportunity by grouping close short trips?
  val passengerCountDF = taxiDF.filter(col("passenger_count")<3).select(count("*"))
  passengerCountDF.show()
  taxiDF.select(count("*")).show()

  val groupAttemptsDF = taxiDF
    .select(
      round(unix_timestamp(col("tpep_pickup_datetime")) / 300).cast("integer").as("fiveMinId"),
      col("PULocationID"),
      col("total_amount")
    )
    .filter(col("passenger_count") < 3)
    .groupBy(col("fiveMinId"), col("PULocationID"))
    .agg(count("*").as("total_trips"), sum(col("total_amount")).as("total_amount"))
    //    .orderBy(col("total_trips").desc_nulls_last)
    .withColumn("approximate_datetime", from_unixtime(col("fiveMinId") * 300))
    .drop("fiveMinId")
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("LocationID", "service_zone")
    .orderBy(col("total_trips").desc_nulls_last)

  groupAttemptsDF.show()


  val percentGroupAttempt = 0.05
  val percentAcceptGrouping = 0.3
  val discount = 5
  val extraCost = 2
  val avgCostReduction = 0.6 * taxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0)

  val groupingEstimateEconomicImpactDF = groupAttemptsDF
    .withColumn("groupedRides", col("total_trips") * percentGroupAttempt)
    .withColumn("acceptedGroupedRidesEconomicImpact", col("groupedRides") * percentAcceptGrouping * (avgCostReduction - discount))
    .withColumn("rejectedGroupedRidesEconomicImpact", col("groupedRides") * (1 - percentAcceptGrouping) * extraCost)
    .withColumn("totalImpact", col("acceptedGroupedRidesEconomicImpact") + col("rejectedGroupedRidesEconomicImpact"))

    groupingEstimateEconomicImpactDF.show(100)

  val totalProfitDF = groupingEstimateEconomicImpactDF.select(sum(col("totalImpact")).as("total"))
  totalProfitDF.show()

}
