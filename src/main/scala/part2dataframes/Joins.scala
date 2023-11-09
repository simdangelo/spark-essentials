package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, max}

object Joins extends App {

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
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
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
  guitaristsBandsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))

  // using complex types
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarID)"))

  /**
   * EXERCISES
   *
   * 1. show all employees and their max salary
   * 2. show all employees who were never managers
   * 3. find the job title of the best paid 10 employees in the company
   */

  // reading from a remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagerDF = readTable("dept_manager")
  val titlesDF = readTable("titles")
//
//  employeesDF.show()
//  salariesDF.show()
//  deptManagerDF.show()
//  titlesDF.show()

  // Exercise1
  val maxSalariesPerEmpNoDF = salariesDF.groupBy("emp_no").agg(max("salary").as("maxSalary"))
  val employeesSalariesDF = employeesDF.join(maxSalariesPerEmpNoDF, "emp_no")

  // Exercise2
  val empNeverManagersDF = employeesDF.join(deptManagerDF,
    employeesDF.col("emp_no") === deptManagerDF.col("emp_no"),
    "left_anti")

  // Exercise3
  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeesDF = employeesSalariesDF.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobsDF = bestPaidEmployeesDF.join(mostRecentJobTitlesDF, "emp_no")

  bestPaidJobsDF.show()
}